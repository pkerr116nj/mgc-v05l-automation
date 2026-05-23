"""CLI for the manual paper-only IBKR strategy bridge."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ..execution.ibkr_paper_strategy_bridge import (
    IbkrPaperStrategyBridgeConfig,
    run_ibkr_paper_strategy_bridge,
    write_ibkr_paper_strategy_bridge_artifacts,
    write_strategy_order_intent_schema_file,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "ibkr_paper_strategy_bridge"


class IbkrPaperStrategyBridgeCliError(RuntimeError):
    """Raised when CLI inputs are invalid."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ibkr-paper-strategy-bridge")
    parser.add_argument("--mode", default="PAPER", help="Required environment mode lock. Must be PAPER.")
    parser.add_argument("--host", default="127.0.0.1", help="Required environment host lock. Must be 127.0.0.1.")
    parser.add_argument("--port", type=int, default=7497, help="Required environment port lock. Must be 7497.")
    parser.add_argument("--client-id", type=int, default=9211, help="Dedicated paper strategy bridge client id.")
    parser.add_argument("--account-id", default="DUM882026", help="Expected paper account id.")
    parser.add_argument("--strategy-id", default="ATP_COMPANION_V1_ASIA_US", help="Allowed shared strategy identity that generated the paper intent.")
    parser.add_argument("--symbol", default="MGC", help="Phase-1 executable symbol. Approved futures only; unsupported symbols fail closed.")
    parser.add_argument("--contract-month", default="202606", help="Friendly contract month label.")
    parser.add_argument("--action", default="BUY", choices=("BUY", "SELL"), help="Intent action. BUY opens paper long; SELL closes exact long 1.")
    parser.add_argument("--quantity", type=float, default=1.0, help="Only quantity 1 is allowed.")
    parser.add_argument("--order-type", default="LMT", help="Only LMT is allowed.")
    parser.add_argument(
        "--limit-price-model",
        default="DELAYED_ASK_PLUS_1T_MARKETABLE_BUY",
        help="Explicit paper pricing model. Examples: DELAYED_ASK_PLUS_1T_MARKETABLE_BUY or DELAYED_BID_MINUS_1T_MARKETABLE_SELL.",
    )
    parser.add_argument("--time-in-force", default="DAY", help="Only DAY is allowed.")
    parser.add_argument("--reason", default="GC_ASIA_ONLY_CANDIDATE_PAPER_INTENT", help="Operator-visible reason for the strategy bridge intent.")
    parser.add_argument("--risk-tag", action="append", default=[], help="Repeatable risk tag list for the bridge intent.")
    parser.add_argument("--paper-only", action="store_true", help="Required paper-only guard flag. Fails closed if omitted.")
    parser.add_argument("--prepare-manual-submit-bundle", action="store_true", help="Generate a frozen manual submit bundle and approval digest for the current strategy intent.")
    parser.add_argument("--submit", action="store_true", help="Optional delegation into the proven manual paper harness after bridge preflight passes.")
    parser.add_argument("--manual-frozen-preview-path", type=Path, default=None, help="Required manual-harness frozen preview bundle when --submit is used.")
    parser.add_argument("--approval-digest", default=None, help="Required exact manual-harness digest when --submit is used.")
    parser.add_argument("--approval-phrase", default=None, help="Required exact manual-harness approval phrase when --submit is used.")
    parser.add_argument(
        "--pre-action-snapshot-max-age-seconds",
        type=int,
        default=300,
        help="Maximum age for Control Plane Snapshot evidence required before submit-capable bridge delegation.",
    )
    parser.add_argument("--timeout-seconds", type=float, default=15.0, help="Per-request callback timeout.")
    parser.add_argument("--kill-switch-path", type=Path, default=REPO_ROOT / "var" / "ibkr_paper_strategy_bridge.disabled", help="Kill-switch file path checked during preflight.")
    parser.add_argument("--daily-order-cap", type=int, default=1, help="Daily strategy bridge order cap.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Artifact output directory.")
    parser.add_argument("--overwrite", action="store_true", help="Allow replacing a non-empty output directory.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    _ensure_output_dir(output_dir, overwrite=bool(args.overwrite))
    write_strategy_order_intent_schema_file(repo_root=REPO_ROOT)
    config = IbkrPaperStrategyBridgeConfig(
        repo_root=REPO_ROOT,
        mode=str(args.mode or "").strip().upper(),
        host=str(args.host or "").strip(),
        port=int(args.port),
        client_id=int(args.client_id),
        account_id=str(args.account_id or "").strip(),
        strategy_id=str(args.strategy_id or "").strip(),
        symbol=str(args.symbol or "").strip().upper(),
        contract_month=str(args.contract_month or "").strip(),
        action=str(args.action or "").strip().upper(),
        quantity=float(args.quantity),
        order_type=str(args.order_type or "").strip().upper(),
        limit_price_model=str(args.limit_price_model or "").strip().upper(),
        time_in_force=str(args.time_in_force or "").strip().upper(),
        reason=str(args.reason or "").strip(),
        risk_tags=tuple(str(tag or "").strip() for tag in list(args.risk_tag or []) if str(tag or "").strip()),
        paper_only=bool(args.paper_only),
        prepare_manual_submit_bundle=bool(args.prepare_manual_submit_bundle),
        submit=bool(args.submit),
        timeout_seconds=float(args.timeout_seconds),
        kill_switch_path=Path(args.kill_switch_path),
        daily_order_cap=int(args.daily_order_cap),
        output_dir=output_dir,
        approval_digest=str(args.approval_digest or "").strip() or None,
        approval_phrase=str(args.approval_phrase or "").strip() or None,
        manual_frozen_preview_path=Path(args.manual_frozen_preview_path) if args.manual_frozen_preview_path is not None else None,
        pre_action_snapshot_max_age_seconds=int(args.pre_action_snapshot_max_age_seconds),
    )
    artifacts = run_ibkr_paper_strategy_bridge(config=config)
    write_ibkr_paper_strategy_bridge_artifacts(output_dir=output_dir, artifacts=artifacts)
    print(json.dumps(artifacts.report, indent=2, sort_keys=True))
    return artifacts.exit_code


def _ensure_output_dir(output_dir: Path, *, overwrite: bool) -> None:
    if output_dir.exists() and any(output_dir.iterdir()) and not overwrite:
        raise IbkrPaperStrategyBridgeCliError(
            f"Output directory {output_dir} already exists and is not empty. Re-run with --overwrite to replace artifacts."
        )
    output_dir.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
