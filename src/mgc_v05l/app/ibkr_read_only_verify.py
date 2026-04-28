"""Strict IBKR TWS paper read-only verification CLI."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ..execution.ibkr_read_only_verifier import (
    IbkrReadOnlyVerificationConfig,
    verify_ibkr_read_only_connection,
    write_ibkr_read_only_artifacts,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "ibkr_read_only_verification"


class IbkrReadOnlyVerifyCliError(RuntimeError):
    """Raised when CLI inputs are invalid."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ibkr-read-only-verify")
    parser.add_argument("--mode", default="PAPER", help="Required environment mode lock. Must be PAPER for this pass.")
    parser.add_argument("--host", default="127.0.0.1", help="Required environment host lock. Must be 127.0.0.1 for this pass.")
    parser.add_argument("--port", type=int, default=7497, help="Required environment port lock. Must be 7497 for TWS paper.")
    parser.add_argument("--client-id", type=int, default=9071, help="Verifier client id. Defaults to a dedicated read-only probe id.")
    parser.add_argument("--account-id", default=None, help="Optional explicit account id if TWS exposes multiple managed accounts.")
    parser.add_argument("--read-only", action="store_true", help="Required for this pass. Fails closed if omitted.")
    parser.add_argument("--timeout-seconds", type=float, default=15.0, help="Per-check callback timeout.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Artifact output directory.")
    parser.add_argument("--overwrite", action="store_true", help="Allow writing into a non-empty output directory.")
    parser.add_argument("--skip-market-data-probe", action="store_true", help="Skip the read-only market data snapshot probe.")
    parser.add_argument("--skip-duplicate-client-id-probe", action="store_true", help="Skip the concurrent duplicate client-id probe.")
    parser.add_argument("--gc-expiry", default="202606", help="GC contract month in YYYYMM for qualification verification.")
    parser.add_argument("--mgc-expiry", default="202606", help="MGC contract month in YYYYMM for qualification verification.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    _ensure_output_dir(output_dir, overwrite=bool(args.overwrite))

    config = IbkrReadOnlyVerificationConfig(
        repo_root=REPO_ROOT,
        mode=str(args.mode or "").strip().upper(),
        host=str(args.host or "").strip(),
        port=int(args.port),
        client_id=int(args.client_id),
        read_only=bool(args.read_only),
        account_id=str(args.account_id or "").strip() or None,
        timeout_seconds=float(args.timeout_seconds),
        probe_market_data=not bool(args.skip_market_data_probe),
        probe_duplicate_client_id=not bool(args.skip_duplicate_client_id_probe),
        gc_expiry=str(args.gc_expiry or "").strip(),
        mgc_expiry=str(args.mgc_expiry or "").strip(),
    )

    artifacts = verify_ibkr_read_only_connection(config=config)
    write_ibkr_read_only_artifacts(output_dir=output_dir, artifacts=artifacts)
    print(json.dumps(artifacts.connection_report, indent=2, sort_keys=True))
    return artifacts.exit_code


def _ensure_output_dir(output_dir: Path, *, overwrite: bool) -> None:
    if output_dir.exists() and any(output_dir.iterdir()) and not overwrite:
        raise IbkrReadOnlyVerifyCliError(
            f"Output directory {output_dir} already exists and is not empty. Re-run with --overwrite to replace artifacts."
        )
    output_dir.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
