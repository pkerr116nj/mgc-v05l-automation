"""CLI for the read-only IBKR post-manual-close reconciliation pass."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ..execution.ibkr_post_manual_close_reconciliation import (
    IbkrPostManualCloseReconciliationConfig,
    run_ibkr_post_manual_close_reconciliation,
    write_ibkr_post_manual_close_reconciliation_artifacts,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "ibkr_post_manual_close_reconciliation"


class IbkrPostManualCloseReconciliationCliError(RuntimeError):
    """Raised when CLI inputs are invalid."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ibkr-post-manual-close-reconciliation")
    parser.add_argument("--account-id", default="DUM882026", help="Expected paper account id.")
    parser.add_argument("--read-only", action="store_true", help="Required for this pass. Fails closed if omitted.")
    parser.add_argument("--position-reconciliation-report-path", type=Path, default=None, help="Optional override for the live exact-position reconciliation report.")
    parser.add_argument("--observation-dry-run-report-path", type=Path, default=None, help="Optional override for the post-close read-only observation diagnostic report.")
    parser.add_argument("--unattended-close-report-path", type=Path, default=None, help="Optional override for the unattended close test report.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Artifact output directory.")
    parser.add_argument("--overwrite", action="store_true", help="Allow writing into a non-empty output directory.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    _ensure_output_dir(output_dir, overwrite=bool(args.overwrite))
    config = IbkrPostManualCloseReconciliationConfig(
        repo_root=REPO_ROOT,
        account_id=str(args.account_id or "").strip() or "DUM882026",
        read_only=bool(args.read_only),
        position_reconciliation_report_path=Path(args.position_reconciliation_report_path) if args.position_reconciliation_report_path is not None else None,
        observation_dry_run_report_path=Path(args.observation_dry_run_report_path) if args.observation_dry_run_report_path is not None else None,
        unattended_close_report_path=Path(args.unattended_close_report_path) if args.unattended_close_report_path is not None else None,
    )
    artifacts = run_ibkr_post_manual_close_reconciliation(config=config)
    write_ibkr_post_manual_close_reconciliation_artifacts(output_dir=output_dir, artifacts=artifacts)
    print(json.dumps(artifacts.report, indent=2, sort_keys=True))
    return artifacts.exit_code


def _ensure_output_dir(output_dir: Path, *, overwrite: bool) -> None:
    if output_dir.exists() and any(output_dir.iterdir()) and not overwrite:
        raise IbkrPostManualCloseReconciliationCliError(
            f"Output directory {output_dir} already exists and is not empty. Re-run with --overwrite to replace artifacts."
        )
    output_dir.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
