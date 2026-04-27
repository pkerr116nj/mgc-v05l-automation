"""Manual-first IBKR paper read-only account-truth CLI."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..config.ibkr import load_ibkr_config
from ..execution.ibkr_account_truth import (
    IbkrAccountTruthCaptureConfig,
    IbkrPaperModeRequiredError,
    IbkrRealConnectionBlocked,
    capture_fixture_ibkr_paper_truth,
    capture_real_ibkr_paper_truth,
)
from .ibkr_milestone_a_acceptance import evaluate_ibkr_milestone_a_snapshot


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "ibkr_account_truth" / "paper_truth"
SAFETY_MESSAGE = (
    "PAPER READ-ONLY MODE ONLY. This command does not submit, cancel, replace, or flatten orders. "
    "It is intended for IBKR paper account truth validation only. Keep TWS / IB Gateway Read-Only API enabled for this stage."
)


class IbkrAccountTruthCliError(RuntimeError):
    """Raised when CLI inputs are invalid."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ibkr-account-truth")
    parser.add_argument("--mode", default="paper", help="IBKR environment mode. Stage 1 accepts paper only.")
    parser.add_argument("--host", default=None, help="IBKR socket host. Defaults from environment/config.")
    parser.add_argument("--port", type=int, default=None, help="IBKR socket port. Defaults from environment/config.")
    parser.add_argument("--client-id", type=int, default=None, help="IBKR API client id. Defaults from environment/config.")
    parser.add_argument("--account-id", default=None, help="Optional explicit paper account id when managed accounts are ambiguous.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--fixture-json", type=Path, default=None, help="Fixture payload for safe local validation.")
    parser.add_argument("--allow-real-api", action="store_true", help="Allow real IBKR paper read-only socket connection.")
    parser.add_argument("--timeout-seconds", type=float, default=10.0, help="Truth capture timeout for real paper mode.")
    parser.add_argument("--overwrite", action="store_true", help="Allow overwriting an existing output directory.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    _ensure_output_dir(output_dir, overwrite=bool(args.overwrite))
    capture_config = _capture_config_from_args(args)
    payload: dict[str, Any]

    try:
        if args.fixture_json is not None:
            fixture_payload = json.loads(Path(args.fixture_json).read_text(encoding="utf-8"))
            result = capture_fixture_ibkr_paper_truth(
                repo_root=REPO_ROOT,
                capture_config=capture_config,
                fixture_payload=fixture_payload,
                acceptance_evaluator=evaluate_ibkr_milestone_a_snapshot,
            )
        else:
            result = capture_real_ibkr_paper_truth(
                repo_root=REPO_ROOT,
                capture_config=capture_config,
                acceptance_evaluator=evaluate_ibkr_milestone_a_snapshot,
                allow_real_api=bool(args.allow_real_api),
            )
    except Exception as exc:
        _write_audit_failure(output_dir=output_dir, capture_config=capture_config, args=args, detail=str(exc))
        raise

    payload = {
        "status": "ready" if result.acceptance.get("ready") else "warning",
        "operator_safety_message": SAFETY_MESSAGE,
        "mode": capture_config.mode,
        "host": capture_config.host,
        "port": capture_config.port,
        "client_id": capture_config.client_id,
        "selected_account_id": result.snapshot.get("selected_account_id"),
        "snapshot": result.snapshot,
        "acceptance": result.acceptance,
        "audit": result.audit,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    _write_output_artifacts(output_dir=output_dir, payload=payload)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def _capture_config_from_args(args: argparse.Namespace) -> IbkrAccountTruthCaptureConfig:
    ibkr_config = load_ibkr_config()
    mode = str(args.mode or ibkr_config.mode).strip().lower()
    if mode != "paper":
        raise IbkrPaperModeRequiredError("IBKR account-truth Stage 1 supports paper mode only; live mode is rejected.")
    return IbkrAccountTruthCaptureConfig(
        mode=mode,
        host=str(args.host or ibkr_config.host).strip(),
        port=int(args.port or ibkr_config.port),
        client_id=int(args.client_id or ibkr_config.client_id),
        account_id=str(args.account_id or ibkr_config.account_id or "").strip() or None,
        timeout_seconds=float(args.timeout_seconds),
    )


def _ensure_output_dir(output_dir: Path, *, overwrite: bool) -> None:
    reports_dir = output_dir / "reports"
    if reports_dir.exists() and any(reports_dir.iterdir()) and not overwrite:
        raise IbkrAccountTruthCliError(
            f"Output directory {reports_dir} already exists and is not empty. Re-run with --overwrite to replace artifacts."
        )
    reports_dir.mkdir(parents=True, exist_ok=True)


def _write_output_artifacts(*, output_dir: Path, payload: dict[str, Any]) -> None:
    reports_dir = output_dir / "reports"
    snapshot = dict(payload.get("snapshot") or {})
    acceptance = dict(payload.get("acceptance") or {})
    audit = dict(payload.get("audit") or {})
    (reports_dir / "ibkr_account_truth_snapshot.json").write_text(
        json.dumps(snapshot, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (reports_dir / "ibkr_account_truth_acceptance.json").write_text(
        json.dumps(acceptance, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (reports_dir / "ibkr_account_truth_audit.json").write_text(
        json.dumps(audit, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (reports_dir / "ibkr_account_truth_summary.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (reports_dir / "ibkr_account_truth_summary.md").write_text(
        _render_markdown_summary(payload),
        encoding="utf-8",
    )


def _render_markdown_summary(payload: dict[str, Any]) -> str:
    acceptance = dict(payload.get("acceptance") or {})
    checks = dict(acceptance.get("checks") or {})
    lines = [
        "# IBKR Paper Account Truth Summary",
        "",
        f"- status: `{payload.get('status')}`",
        f"- mode: `{payload.get('mode')}`",
        f"- host: `{payload.get('host')}`",
        f"- port: `{payload.get('port')}`",
        f"- client_id: `{payload.get('client_id')}`",
        f"- selected_account_id: `{payload.get('selected_account_id')}`",
        "",
        f"> {payload.get('operator_safety_message')}",
        "",
        "## Acceptance Checks",
        "",
    ]
    for name, row in checks.items():
        lines.append(f"- `{name}`: `{'ok' if row.get('ok') else 'not_ok'}`")
    return "\n".join(lines) + "\n"


def _write_audit_failure(
    *,
    output_dir: Path,
    capture_config: IbkrAccountTruthCaptureConfig,
    args: argparse.Namespace,
    detail: str,
) -> None:
    reports_dir = output_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    audit = {
        "provider_id": "ibkr_execution",
        "mode": capture_config.mode,
        "status": "failed",
        "host": capture_config.host,
        "port": capture_config.port,
        "client_id": capture_config.client_id,
        "selected_account_id": capture_config.account_id,
        "allow_real_api": bool(args.allow_real_api),
        "read_only_only": True,
        "orders_allowed": False,
        "detail": detail,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "operator_safety_message": SAFETY_MESSAGE,
    }
    (reports_dir / "ibkr_account_truth_audit.json").write_text(
        json.dumps(audit, indent=2, sort_keys=True),
        encoding="utf-8",
    )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
