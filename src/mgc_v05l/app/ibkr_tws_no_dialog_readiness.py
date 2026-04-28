"""CLI for the read-only TWS no-dialog readiness preflight."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ..execution.ibkr_tws_no_dialog_readiness import (
    IbkrTwsNoDialogReadinessConfig,
    run_ibkr_tws_no_dialog_readiness_preflight,
    write_ibkr_tws_no_dialog_readiness_artifacts,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "ibkr_tws_no_dialog_readiness"


class IbkrTwsNoDialogReadinessCliError(RuntimeError):
    """Raised when CLI inputs are invalid."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ibkr-tws-no-dialog-readiness")
    parser.add_argument("--mode", default="PAPER", help="Required environment mode lock. Must be PAPER.")
    parser.add_argument("--host", default="127.0.0.1", help="Required environment host lock. Must be 127.0.0.1.")
    parser.add_argument("--port", type=int, default=7497, help="Required environment port lock. Must be 7497 for TWS paper.")
    parser.add_argument("--client-id", type=int, default=9171, help="Dedicated read-only readiness client id.")
    parser.add_argument("--account-id", default="DUM882026", help="Expected paper account id.")
    parser.add_argument("--read-only", action="store_true", help="Required for this preflight. Fails closed if omitted.")
    parser.add_argument("--timeout-seconds", type=float, default=15.0, help="Per-request callback timeout.")
    parser.add_argument("--caller-path", default="manual_cli", help="Expected caller boundary. Strategy-style callers fail closed.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Artifact output directory.")
    parser.add_argument("--overwrite", action="store_true", help="Allow writing into a non-empty output directory.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    _ensure_output_dir(output_dir, overwrite=bool(args.overwrite))
    config = IbkrTwsNoDialogReadinessConfig(
        repo_root=REPO_ROOT,
        mode=str(args.mode or "").strip().upper(),
        host=str(args.host or "").strip(),
        port=int(args.port),
        client_id=int(args.client_id),
        read_only=bool(args.read_only),
        account_id=str(args.account_id or "").strip() or None,
        timeout_seconds=float(args.timeout_seconds),
        caller_path=str(args.caller_path or "").strip() or "manual_cli",
    )
    artifacts = run_ibkr_tws_no_dialog_readiness_preflight(config=config)
    write_ibkr_tws_no_dialog_readiness_artifacts(output_dir=output_dir, artifacts=artifacts)
    print(json.dumps(artifacts.report, indent=2, sort_keys=True))
    return artifacts.exit_code


def _ensure_output_dir(output_dir: Path, *, overwrite: bool) -> None:
    if output_dir.exists() and any(output_dir.iterdir()) and not overwrite:
        raise IbkrTwsNoDialogReadinessCliError(
            f"Output directory {output_dir} already exists and is not empty. Re-run with --overwrite to replace artifacts."
        )
    output_dir.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
