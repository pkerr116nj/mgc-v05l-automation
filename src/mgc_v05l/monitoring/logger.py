"""Structured logger placeholder."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any


def get_logger(name: str) -> logging.Logger:
    """Return a standard logger pending structured logging configuration."""
    return logging.getLogger(name)


class StructuredLogger:
    """Produces bar, order, reconciliation, and fault audit records."""

    def __init__(self, artifact_dir: str | Path) -> None:
        self._artifact_dir = Path(artifact_dir)
        self._artifact_dir.mkdir(parents=True, exist_ok=True)
        self._logger = get_logger(__name__)

    @property
    def artifact_dir(self) -> Path:
        return self._artifact_dir

    def log_branch_source(self, payload: dict[str, Any]) -> Path:
        return self._append_jsonl("branch_sources.jsonl", payload)

    def log_rule_block(self, payload: dict[str, Any]) -> Path:
        return self._append_jsonl("rule_blocks.jsonl", payload)

    def log_alert(self, payload: dict[str, Any]) -> Path:
        return self._append_jsonl("alerts.jsonl", payload)

    def write_alert_state(self, payload: dict[str, Any]) -> Path:
        return self._write_json("alerts_state.json", payload)

    def log_reconciliation_event(self, payload: dict[str, Any]) -> Path:
        return self._append_jsonl("reconciliation_events.jsonl", payload)

    def log_execution_watchdog_event(self, payload: dict[str, Any]) -> Path:
        return self._append_jsonl("execution_watchdog_events.jsonl", payload)

    def log_restore_validation_event(self, payload: dict[str, Any]) -> Path:
        return self._append_jsonl("restore_validation_events.jsonl", payload)

    def log_paper_soak_validation_event(self, payload: dict[str, Any]) -> Path:
        return self._append_jsonl("paper_soak_validation_events.jsonl", payload)

    def log_paper_soak_extended_event(self, payload: dict[str, Any]) -> Path:
        return self._append_jsonl("paper_soak_extended_events.jsonl", payload)

    def log_paper_soak_unattended_event(self, payload: dict[str, Any]) -> Path:
        return self._append_jsonl("paper_soak_unattended_events.jsonl", payload)

    def log_exit_parity_event(self, payload: dict[str, Any]) -> Path:
        return self._append_jsonl("exit_parity_events.jsonl", payload)

    def log_live_timing_event(self, payload: dict[str, Any]) -> Path:
        return self._append_jsonl("live_timing_events.jsonl", payload)

    def log_live_shadow_event(self, payload: dict[str, Any]) -> Path:
        return self._append_jsonl("live_shadow_events.jsonl", payload)

    def log_live_strategy_pilot_event(self, payload: dict[str, Any]) -> Path:
        return self._append_jsonl("live_strategy_pilot_events.jsonl", payload)

    def log_live_strategy_pilot_cycle_event(self, payload: dict[str, Any]) -> Path:
        return self._append_jsonl("live_strategy_pilot_cycle_events.jsonl", payload)

    def log_operator_control(self, payload: dict[str, Any]) -> Path:
        return self._append_jsonl("operator_controls.jsonl", payload)

    def write_operator_status(self, payload: dict[str, Any]) -> Path:
        return self._write_json("operator_status.json", payload)

    def write_restore_validation_state(self, payload: dict[str, Any]) -> Path:
        return self._write_json("restore_validation_latest.json", payload)

    def write_paper_soak_validation_state(self, payload: dict[str, Any]) -> Path:
        return self._write_json("paper_soak_validation_latest.json", payload)

    def write_paper_soak_extended_state(self, payload: dict[str, Any]) -> Path:
        return self._write_json("paper_soak_extended_latest.json", payload)

    def write_paper_soak_unattended_state(self, payload: dict[str, Any]) -> Path:
        return self._write_json("paper_soak_unattended_latest.json", payload)

    def write_exit_parity_state(self, payload: dict[str, Any]) -> Path:
        return self._write_json("exit_parity_summary_latest.json", payload)

    def write_live_timing_state(self, payload: dict[str, Any]) -> Path:
        return self._write_json("live_timing_summary_latest.json", payload)

    def write_live_shadow_state(self, payload: dict[str, Any]) -> Path:
        return self._write_json("live_shadow_summary_latest.json", payload)

    def write_live_strategy_pilot_state(self, payload: dict[str, Any]) -> Path:
        return self._write_json("live_strategy_pilot_summary_latest.json", payload)

    def write_live_strategy_pilot_cycle_state(self, payload: dict[str, Any]) -> Path:
        return self._write_json("live_strategy_pilot_cycle_latest.json", payload)

    def write_live_strategy_signal_observability_state(self, payload: dict[str, Any]) -> Path:
        return self._write_json("live_strategy_signal_observability_latest.json", payload)

    def write_live_timing_validation_state(self, payload: dict[str, Any]) -> Path:
        return self._write_json("paper_live_timing_validation_latest.json", payload)

    def write_parity_report(self, report_name: str, payload: dict[str, Any]) -> Path:
        parity_dir = self._artifact_dir / "parity"
        parity_dir.mkdir(parents=True, exist_ok=True)
        return self._write_json(parity_dir / f"{report_name}.json", payload)

    def _append_jsonl(self, file_name: str, payload: dict[str, Any]) -> Path:
        path = self._artifact_dir / file_name
        record = dict(payload)
        record.setdefault("logged_at", datetime.now(timezone.utc).isoformat())
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True, default=_json_default))
            handle.write("\n")
        return path

    def _write_json(self, file_name_or_path: str | Path, payload: dict[str, Any]) -> Path:
        path = file_name_or_path if isinstance(file_name_or_path, Path) else self._artifact_dir / file_name_or_path
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, indent=2, default=_json_default)
            handle.write("\n")
        return path


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "value"):
        return value.value
    return str(value)
