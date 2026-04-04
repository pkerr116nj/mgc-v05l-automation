"""Compact promotion memo for widened strategy-universe retest output."""

from __future__ import annotations

import argparse
import json
import math
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_REPORT_PATH = REPO_ROOT / "outputs" / "reports" / "strategy_universe_retest" / "strategy_universe_retest.json"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "strategy_universe_retest"


def generate_strategy_decision_memo(
    *,
    report_path: str | Path = DEFAULT_REPORT_PATH,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
) -> dict[str, Path]:
    resolved_report_path = Path(report_path).resolve()
    resolved_output_dir = Path(output_dir).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    payload = json.loads(resolved_report_path.read_text(encoding="utf-8"))
    rows = list(payload.get("results") or [])

    promotable = [row for row in rows if str(row.get("bucket") or "") == "promotable_now"]
    retained = [row for row in rows if str(row.get("bucket") or "") == "retained_candidate"]
    reject = [row for row in rows if str(row.get("bucket") or "") == "still_reject"]
    strongest_retained = max(
        retained,
        key=lambda row: float(((row.get("metrics") or {}).get("net_pnl")) or float("-inf")),
        default=None,
    )

    memo = {
        "generated_at": datetime.now(UTC).isoformat(),
        "report_path": str(resolved_report_path),
        "promotable_now": [_memo_row(row, include_action=True) for row in promotable],
        "retained_candidate": [
            _memo_row(
                row,
                blocker=(
                    "Quantity improved, but VWAP quality still degrades against the prior method."
                    if row is strongest_retained and str(row.get("material_improvement") or "") == "quantity_up_quality_down"
                    else "Economics remain positive enough to keep under study, but not clean enough for promotion."
                ),
                improvement_needed=(
                    "Promote once favorable/neutral mix improves and PF holds above current level over the longer base."
                    if row is strongest_retained
                    else None
                ),
            )
            for row in retained
        ],
        "still_reject": [_memo_row(row) for row in reject],
        "strongest_retained_candidate": _memo_row(
            strongest_retained,
            blocker="VWAP quality mix and quantity-quality tradeoff still block promotion.",
            improvement_needed="Needs cleaner favorable-vs-neutral execution quality without giving back PF or drawdown.",
        )
        if strongest_retained is not None
        else None,
    }
    json_path = resolved_output_dir / "strategy_decision_memo.json"
    markdown_path = resolved_output_dir / "strategy_decision_memo.md"
    json_path.write_text(json.dumps(memo, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(memo).strip() + "\n", encoding="utf-8")
    return {"json_path": json_path, "markdown_path": markdown_path}


def _memo_row(
    row: dict[str, Any] | None,
    *,
    include_action: bool = False,
    blocker: str | None = None,
    improvement_needed: str | None = None,
) -> dict[str, Any] | None:
    if row is None:
        return None
    metrics = dict(row.get("metrics") or {})
    prior = dict(row.get("prior_method_comparison") or {})
    vwap = dict(metrics.get("vwap_breakdown") or {})
    result = {
        "strategy_id": row.get("strategy_id"),
        "display_name": row.get("display_name"),
        "status": row.get("status"),
        "bucket": row.get("bucket"),
        "trades": metrics.get("trade_count"),
        "net_pnl": metrics.get("net_pnl"),
        "profit_factor": _json_safe_number(metrics.get("profit_factor")),
        "vwap_breakdown": vwap,
        "why_bucketed": row.get("recommendation"),
        "changed_vs_prior_run": {
            "current_trade_count": metrics.get("trade_count"),
            "prior_trade_count": prior.get("trade_count"),
            "current_net_pnl": metrics.get("net_pnl"),
            "prior_net_pnl": prior.get("net_pnl"),
            "current_profit_factor": _json_safe_number(metrics.get("profit_factor")),
            "prior_profit_factor": _json_safe_number(prior.get("profit_factor")),
            "material_improvement": row.get("material_improvement"),
        },
        "longer_history_effect": (
            "Strengthened"
            if float(metrics.get("net_pnl") or 0) > float(prior.get("net_pnl") or 0)
            else "Weakened"
            if float(metrics.get("net_pnl") or 0) < float(prior.get("net_pnl") or 0)
            else "Mixed / unchanged"
        ),
    }
    if include_action:
        result["proposed_registry_action"] = "Add to approved registry with written promotion note; do not silently promote."
        result["remaining_caveat"] = "Confirm the longer-history result still holds after final operator review."
    if blocker is not None:
        result["promotion_blocker"] = blocker
    if improvement_needed is not None:
        result["what_must_improve_for_promotion"] = improvement_needed
    return result


def _json_safe_number(value: Any) -> Any:
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, str) and value.strip().lower() in {"inf", "infinity", "nan", "+inf", "-inf", "+infinity", "-infinity"}:
        return None
    return value


def _render_markdown(memo: dict[str, Any]) -> str:
    lines = [
        "# Strategy Decision Memo",
        "",
        f"- Generated At: {memo.get('generated_at')}",
        f"- Source Report: {memo.get('report_path')}",
        "",
    ]
    for section in ("promotable_now", "retained_candidate", "still_reject"):
        rows = memo.get(section) or []
        lines.append(f"## {section.replace('_', ' ').title()}")
        if not rows:
            lines.append("")
            lines.append("- None.")
            lines.append("")
            continue
        lines.append("")
        for row in rows:
            lines.append(f"- {row.get('display_name')} ({row.get('strategy_id')})")
            lines.append(
                f"  Trades={row.get('trades')} | Net={row.get('net_pnl')} | PF={row.get('profit_factor')} | "
                f"VWAP={json.dumps(row.get('vwap_breakdown') or {}, sort_keys=True)}"
            )
            lines.append(f"  Bucket reason={row.get('why_bucketed')}")
            lines.append(f"  Longer-history effect={row.get('longer_history_effect')}")
            if row.get("proposed_registry_action"):
                lines.append(f"  Proposed action={row.get('proposed_registry_action')}")
                lines.append(f"  Remaining caveat={row.get('remaining_caveat')}")
            if row.get("promotion_blocker"):
                lines.append(f"  Blocker={row.get('promotion_blocker')}")
            if row.get("what_must_improve_for_promotion"):
                lines.append(f"  Improve={row.get('what_must_improve_for_promotion')}")
        lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(prog="strategy-decision-memo")
    parser.add_argument("--report-path", default=str(DEFAULT_REPORT_PATH))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args()
    outputs = generate_strategy_decision_memo(report_path=args.report_path, output_dir=args.output_dir)
    print(json.dumps({key: str(value) for key, value in outputs.items()}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
