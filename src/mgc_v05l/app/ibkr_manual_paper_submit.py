"""CLI for the single-order IBKR manual paper submit/cancel test harness."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ..execution.ibkr_manual_paper_submit import (
    IbkrManualPaperSubmitConfig,
    run_ibkr_order_observation_diagnostic,
    run_ibkr_manual_paper_submit_test,
    write_ibkr_order_observation_diagnostic_artifacts,
    write_ibkr_manual_paper_submit_artifacts,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "ibkr_manual_paper_submit"


class IbkrManualPaperSubmitCliError(RuntimeError):
    """Raised when CLI inputs are invalid."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ibkr-manual-paper-submit")
    parser.add_argument("--mode", default="PAPER", help="Required environment mode lock. Must be PAPER.")
    parser.add_argument("--host", default="127.0.0.1", help="Required environment host lock. Must be 127.0.0.1.")
    parser.add_argument("--port", type=int, default=7497, help="Required environment port lock. Must be 7497 for TWS paper.")
    parser.add_argument("--client-id", type=int, default=9074, help="Dedicated manual paper submit client id.")
    parser.add_argument("--account-id", default=None, help="Optional expected account id. The harness fails closed if it does not match.")
    parser.add_argument("--symbol", default="MGC", help="Initial scope only allows MGC.")
    parser.add_argument("--expiry", default="202606", help="Initial scope only allows 202606.")
    parser.add_argument("--action", default="BUY", help="Manual paper fill uses BUY; manual paper close uses SELL.")
    parser.add_argument("--quantity", type=float, default=1.0, help="Initial scope only allows quantity 1.")
    parser.add_argument("--order-type", default="LMT", help="Initial scope only allows LMT.")
    parser.add_argument("--limit-price", type=float, default=None, help="Optional preview limit price. PAPER_FILL_TEST derives a marketable limit from the delayed ask/last when omitted.")
    parser.add_argument("--time-in-force", default="DAY", help="Initial scope only allows DAY.")
    parser.add_argument("--test-mode", default="PAPER_RESTING_TEST", choices=("PAPER_RESTING_TEST", "PAPER_FILL_TEST", "PAPER_CLOSE_TEST"), help="Manual test mode. PAPER_FILL_TEST is intended for one controlled paper fill attempt. PAPER_CLOSE_TEST is intended for one controlled paper close attempt.")
    parser.add_argument("--timeout-seconds", type=float, default=15.0, help="Truth refresh and lifecycle timeout in seconds.")
    parser.add_argument("--fill-timeout-seconds", type=float, default=60.0, help="How long PAPER_FILL_TEST waits for broker-truth fill verification before canceling.")
    parser.add_argument("--post-approval-observation-seconds", type=float, default=75.0, help="How long the manual harness keeps observing broker truth after TWS approval before declaring a fill-timeout path.")
    parser.add_argument("--fill-limit-offset-ticks", type=float, default=1.0, help="Tick offset applied above delayed ask/last for PAPER_FILL_TEST BUY previews and below delayed bid/last for PAPER_CLOSE_TEST SELL previews.")
    parser.add_argument(
        "--manual-confirmation-timeout-seconds",
        type=float,
        default=90.0,
        help="How long the manual harness waits for the operator to handle the TWS confirmation dialog.",
    )
    parser.add_argument("--submit", action="store_true", help="Explicitly attempt one paper submit/cancel lifecycle after preview and approval validation.")
    parser.add_argument("--explicit-operator-submit", action="store_true", help="Required one-shot operator flag for any manual harness broker mutation. Defaults to dry-run/no-submit.")
    parser.add_argument("--approval-digest", default=None, help="Exact preview digest required for submit.")
    parser.add_argument("--approval-phrase", default=None, help="Exact typed confirmation phrase required for submit.")
    parser.add_argument("--order-ref", default=None, help="Optional explicit IBKR orderRef for the single approved PAPER test order.")
    parser.add_argument(
        "--pre-action-snapshot-max-age-seconds",
        type=int,
        default=300,
        help="Maximum age for the Control Plane Snapshot required before any apply-mode submit.",
    )
    parser.add_argument("--diagnostic-dry-run", action="store_true", help="Do not submit anything. Instead exercise the callback and order-observation stack and write a diagnostic report.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Artifact output directory.")
    parser.add_argument("--frozen-preview-path", type=Path, default=None, help="Optional explicit frozen preview bundle path. Submit requires a previously generated bundle.")
    parser.add_argument("--overwrite", action="store_true", help="Allow replacing a non-empty output directory.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    _ensure_output_dir(output_dir, overwrite=bool(args.overwrite))
    config = IbkrManualPaperSubmitConfig(
        repo_root=REPO_ROOT,
        mode=str(args.mode or "").strip().upper(),
        host=str(args.host or "").strip(),
        port=int(args.port),
        client_id=int(args.client_id),
        account_id=str(args.account_id or "").strip() or None,
        symbol=str(args.symbol or "").strip().upper(),
        expiry=str(args.expiry or "").strip(),
        action=str(args.action or "").strip().upper(),
        quantity=float(args.quantity),
        order_type=str(args.order_type or "").strip().upper(),
        limit_price=float(args.limit_price) if args.limit_price is not None else None,
        time_in_force=str(args.time_in_force or "").strip().upper(),
        test_mode=str(args.test_mode or "").strip().upper(),
        timeout_seconds=float(args.timeout_seconds),
        fill_timeout_seconds=float(args.fill_timeout_seconds),
        post_approval_observation_seconds=float(args.post_approval_observation_seconds),
        fill_limit_offset_ticks=float(args.fill_limit_offset_ticks),
        manual_confirmation_timeout_seconds=float(args.manual_confirmation_timeout_seconds),
        caller_path="manual_cli",
        submit=bool(args.submit),
        explicit_operator_submit=bool(args.explicit_operator_submit),
        approval_digest=str(args.approval_digest or "").strip() or None,
        approval_phrase=str(args.approval_phrase or "").strip() or None,
        order_ref=str(args.order_ref or "").strip() or None,
        pre_action_snapshot_max_age_seconds=int(args.pre_action_snapshot_max_age_seconds),
        output_dir=output_dir,
        frozen_preview_path=Path(args.frozen_preview_path) if args.frozen_preview_path is not None else None,
        diagnostic_dry_run=bool(args.diagnostic_dry_run),
    )
    if config.diagnostic_dry_run:
        report, callback_timeline = run_ibkr_order_observation_diagnostic(config=config)
        write_ibkr_order_observation_diagnostic_artifacts(
            output_dir=output_dir,
            report=report,
            callback_timeline=callback_timeline,
        )
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0 if report.get("classification") != "IBKR_ORDER_OBSERVATION_BLOCKED" else 1
    artifacts = run_ibkr_manual_paper_submit_test(config=config)
    write_ibkr_manual_paper_submit_artifacts(output_dir=output_dir, artifacts=artifacts)
    print(json.dumps(artifacts.report, indent=2, sort_keys=True))
    return artifacts.exit_code


def _ensure_output_dir(output_dir: Path, *, overwrite: bool) -> None:
    if output_dir.exists() and any(output_dir.iterdir()) and not overwrite:
        raise IbkrManualPaperSubmitCliError(
            f"Output directory {output_dir} already exists and is not empty. Re-run with --overwrite to replace artifacts."
        )
    output_dir.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
