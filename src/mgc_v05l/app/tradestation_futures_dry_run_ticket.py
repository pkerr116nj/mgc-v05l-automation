"""Offline Stage 2 TradeStation futures dry-run ticket generation."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from ..execution.tradestation_dry_run import generate_stage2_futures_dry_run


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="tradestation-futures-dry-run-ticket")
    parser.add_argument("--environment", default="sim", help="Execution environment. Stage 2 supports sim only.")
    parser.add_argument("--signal-json", type=Path, required=True, help="Signal JSON payload path.")
    parser.add_argument("--truth-json", type=Path, required=True, help="Normalized read-only truth JSON path.")
    parser.add_argument("--broker-symbol-map", type=Path, required=True, help="Broker symbol resolution JSON path.")
    parser.add_argument("--pending-ticket-registry", type=Path, default=None, help="Optional pending ticket registry JSON path.")
    parser.add_argument("--output-dir", type=Path, required=True, help="Output directory for ticket artifacts.")
    parser.add_argument("--overwrite", action="store_true", help="Allow overwriting an existing output directory.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    if output_dir.exists() and any(output_dir.iterdir()) and not args.overwrite:
        raise RuntimeError(f"Output directory {output_dir} already exists and is not empty. Use --overwrite to replace artifacts.")
    output_dir.mkdir(parents=True, exist_ok=True)
    signal_payload = _read_json(args.signal_json)
    signal_payload.setdefault("environment", str(args.environment))
    truth_payload = _read_json(args.truth_json)
    broker_symbol_map_payload = _read_json(args.broker_symbol_map)
    pending_ticket_registry = _read_json(args.pending_ticket_registry) if args.pending_ticket_registry is not None else {}
    result = generate_stage2_futures_dry_run(
        signal_payload=signal_payload,
        truth_payload=truth_payload,
        broker_symbol_map_payload=broker_symbol_map_payload,
        pending_ticket_registry=pending_ticket_registry,
    )
    payload = result.to_dict()
    _write_artifacts(output_dir=output_dir, payload=payload)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected JSON object at {path}.")
    return payload


def _write_artifacts(*, output_dir: Path, payload: dict[str, Any]) -> None:
    reports_dir = output_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "tradestation_futures_dry_run_summary.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (reports_dir / "tradestation_futures_dry_run_summary.md").write_text(
        _render_markdown(payload),
        encoding="utf-8",
    )


def _render_markdown(payload: dict[str, Any]) -> str:
    trace = dict(payload.get("trace") or {})
    lines = [
        "# TradeStation Futures Dry-Run",
        "",
        f"- Validation status: `{payload.get('validation_status')}`",
        f"- Signal ID: `{trace.get('signal_id')}`",
        f"- Ticket ID: `{trace.get('ticket_id')}`",
        f"- Dry-run payload ID: `{trace.get('dry_run_payload_id')}`",
        f"- Truth freshness threshold seconds: `{payload.get('truth_freshness_threshold_seconds')}`",
    ]
    rejections = list(payload.get("rejections") or [])
    warnings = list(payload.get("warnings") or [])
    if rejections:
        lines.extend(["", "## Rejections", ""])
        lines.extend(f"- {row}" for row in rejections)
    if warnings:
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- {row}" for row in warnings)
    ticket = payload.get("ticket")
    if isinstance(ticket, dict):
        lines.extend(
            [
                "",
                "## Ticket",
                "",
                f"- Symbol: `{ticket.get('internal_symbol')}`",
                f"- Broker symbol: `{ticket.get('broker_symbol')}`",
                f"- Account ID: `{ticket.get('account_id')}`",
                f"- Decision state: `{ticket.get('decision_state')}`",
                f"- Timing bucket: `{ticket.get('timing_bucket')}`",
                f"- Entry order type: `{ticket.get('entry_order_type')}`",
            ]
        )
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    raise SystemExit(main())
