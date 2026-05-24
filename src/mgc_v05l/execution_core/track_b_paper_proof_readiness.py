"""Read-only Track B PAPER round-trip proof readiness.

This command is the runbook gate for supervised PAPER proof attempts. It uses
execution_core authority artifacts only; dashboard projections are never
consumed as proof readiness authority.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.phase1_runtime_data_readiness import (
    Phase1RuntimeDataReadinessConfig,
    build_phase1_runtime_data_readiness,
    write_phase1_runtime_data_readiness_artifacts,
)
from mgc_v05l.execution_core.track_b_runtime_environment_truth import (
    RUNTIME_ACTIVE_OBSERVATION_ONLY,
    RUNTIME_ACTIVE_TRADE_CAPABLE,
)
from mgc_v05l.execution_core.track_b_shared_truth_refresh_cli import (
    DEFAULT_LEASE_HISTORY,
    TrackBSharedTruthRefreshConfig,
    build_runtime_start_preflight_summary,
    refresh_track_b_shared_truth,
)
from mgc_v05l.market_data.phase1_market_session import MARKET_CLOSED_NO_FRESH_BARS


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "proof_readiness"
    / "latest_track_b_paper_proof_readiness.json"
)
READY_FOR_PROOF = "READY_FOR_PROOF"
SHARED_TRUTH_BLOCKED = "SHARED_TRUTH_BLOCKED"
PHASE1_DATA_UNHEALTHY = "PHASE1_DATA_UNHEALTHY"
RUNTIME_ALREADY_ACTIVE = "RUNTIME_ALREADY_ACTIVE"
BROKER_STATE_UNSAFE = "BROKER_STATE_UNSAFE"


@dataclass(frozen=True)
class TrackBPaperProofReadinessConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_OUTPUT_PATH
    required_symbols: tuple[str, ...] = ("MGC", "MNQ")
    required_timeframes: tuple[str, ...] = ("1m", "5m")
    now: datetime | None = None
    broker_lease_history_path: Path | None = DEFAULT_LEASE_HISTORY

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_paper_proof_readiness(
    *,
    config: TrackBPaperProofReadinessConfig,
    now: datetime | None = None,
    pid_running: Callable[[int], bool] | None = None,
    process_root_resolver: Callable[[int], Path | None] | None = None,
    source_commit_resolver: Callable[[Path], str | None] | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or config.now or datetime.now(UTC))
    shared_config = TrackBSharedTruthRefreshConfig(
        repo_root=config.repo_root,
        broker_lease_history_path=config.broker_lease_history_path,
    )
    shared_truth = refresh_track_b_shared_truth(
        config=shared_config,
        now=actual_now,
        pid_running=pid_running,
        process_root_resolver=process_root_resolver,
        source_commit_resolver=source_commit_resolver,
    )
    shared_preflight = build_runtime_start_preflight_summary(shared_truth)
    phase1_config = Phase1RuntimeDataReadinessConfig(repo_root=config.repo_root, now=actual_now)
    phase1 = build_phase1_runtime_data_readiness(config=phase1_config)
    write_phase1_runtime_data_readiness_artifacts(config=phase1_config, artifacts=phase1)
    phase1_required_checks = _required_phase1_checks(
        rows=phase1.rows,
        symbols=config.required_symbols,
        timeframes=config.required_timeframes,
    )
    decision = _classify_proof_readiness(
        shared_truth=shared_truth,
        shared_preflight=shared_preflight,
        phase1_checks=phase1_required_checks,
    )
    payload = {
        "schema_version": "track_b_paper_proof_readiness_v1",
        "generated_at": actual_now.isoformat(),
        "mode": "PAPER",
        "read_only": True,
        "submit_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "classification": decision["classification"],
        "ready_for_proof": decision["classification"] == READY_FOR_PROOF,
        "primary_blocker": decision["primary_blocker"],
        "secondary_warnings": decision["secondary_warnings"],
        "broker_lease_warning": decision["broker_lease_warning"],
        "phase1_session_reason": decision["phase1_session_reason"],
        "blockers": decision["blockers"],
        "shared_truth_preflight": shared_preflight,
        "shared_truth_classifications": _mapping(shared_truth.get("classifications")),
        "phase1_market_session": _mapping(phase1.report.get("market_session")),
        "phase1_required_symbols": list(config.required_symbols),
        "phase1_required_timeframes": list(config.required_timeframes),
        "phase1_required_checks": phase1_required_checks,
        "artifact_paths": {
            "proof_readiness": str(config.resolve(config.output_path)),
            "phase1_runtime_data_readiness": str(
                config.repo_root
                / "outputs"
                / "reports"
                / "phase1_runtime_data_readiness"
                / "latest_phase1_runtime_data_readiness.json"
            ),
            **_mapping(shared_truth.get("artifact_paths")),
        },
    }
    return payload


def write_track_b_paper_proof_readiness(
    *,
    config: TrackBPaperProofReadinessConfig,
    payload: Mapping[str, Any],
) -> Path:
    output_path = config.resolve(config.output_path)
    write_json_atomic(output_path, payload)
    return output_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Check read-only Track B PAPER round-trip proof readiness.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--output-path", default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument("--now", default=None, help="UTC/ISO timestamp override for deterministic dry-run tests.")
    parser.add_argument("--json", action="store_true", help="Print JSON instead of a concise readiness summary.")
    parser.add_argument("--no-broker-lease-history", action="store_true", help="Skip broker lease history append.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    repo_root = Path(args.repo_root).expanduser().resolve()
    output_path = Path(str(args.output_path))
    config = TrackBPaperProofReadinessConfig(
        repo_root=repo_root,
        output_path=output_path,
        now=_parse_datetime(args.now),
        broker_lease_history_path=None if bool(args.no_broker_lease_history) else DEFAULT_LEASE_HISTORY,
    )
    payload = build_track_b_paper_proof_readiness(config=config)
    write_track_b_paper_proof_readiness(config=config, payload=payload)
    if bool(args.json):
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        _print_summary(payload)
    return 0 if payload.get("classification") == READY_FOR_PROOF else 2


def _classify_proof_readiness(
    *,
    shared_truth: Mapping[str, Any],
    shared_preflight: Mapping[str, Any],
    phase1_checks: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    classifications = _mapping(shared_truth.get("classifications"))
    runtime_classification = str(classifications.get("Runtime Environment Truth") or "")
    shared_blockers = [dict(blocker) for blocker in _list(shared_preflight.get("blockers"))]
    broker_lease_warning = _broker_lease_warning(classifications=classifications, blockers=shared_blockers)
    hard_shared_blockers = [
        blocker for blocker in shared_blockers if not _is_degraded_broker_lease_blocker(blocker)
    ]
    missing_or_unready = [dict(check) for check in phase1_checks if check.get("ready") is not True]
    phase1_closed_market = bool(missing_or_unready) and all(
        str(check.get("reason") or "") == MARKET_CLOSED_NO_FRESH_BARS for check in missing_or_unready
    )
    phase1_blockers = _phase1_blockers(missing_or_unready)
    phase1_session_reason = _phase1_session_reason(phase1_checks=phase1_checks)

    if runtime_classification in {RUNTIME_ACTIVE_TRADE_CAPABLE, RUNTIME_ACTIVE_OBSERVATION_ONLY}:
        blockers = [
            {
                "code": "runtime_already_active",
                "detail": f"Runtime Environment Truth is {runtime_classification}; proof starts require no active runtime.",
            }
        ]
        return _decision(
            classification=RUNTIME_ALREADY_ACTIVE,
            blockers=blockers,
            secondary_warnings=_secondary_warnings(shared_blockers=shared_blockers, phase1_blockers=phase1_blockers),
            broker_lease_warning=broker_lease_warning,
            phase1_session_reason=phase1_session_reason,
        )

    if hard_shared_blockers:
        classification = BROKER_STATE_UNSAFE if _broker_state_unsafe(hard_shared_blockers) else SHARED_TRUTH_BLOCKED
        return _decision(
            classification=classification,
            blockers=hard_shared_blockers,
            secondary_warnings=_secondary_warnings(
                shared_blockers=[
                    blocker for blocker in shared_blockers if blocker not in hard_shared_blockers
                ],
                phase1_blockers=phase1_blockers,
            ),
            broker_lease_warning=broker_lease_warning,
            phase1_session_reason=phase1_session_reason,
        )

    if phase1_closed_market:
        return _decision(
            classification=MARKET_CLOSED_NO_FRESH_BARS,
            blockers=phase1_blockers,
            secondary_warnings=_secondary_warnings(shared_blockers=shared_blockers, phase1_blockers=[]),
            broker_lease_warning=broker_lease_warning,
            phase1_session_reason=phase1_session_reason,
        )

    if shared_blockers:
        classification = BROKER_STATE_UNSAFE if _broker_state_unsafe(shared_blockers) else SHARED_TRUTH_BLOCKED
        return _decision(
            classification=classification,
            blockers=shared_blockers,
            secondary_warnings=[],
            broker_lease_warning=broker_lease_warning,
            phase1_session_reason=phase1_session_reason,
        )

    if missing_or_unready:
        return _decision(
            classification=PHASE1_DATA_UNHEALTHY,
            blockers=phase1_blockers,
            secondary_warnings=[],
            broker_lease_warning=broker_lease_warning,
            phase1_session_reason=phase1_session_reason,
        )

    return _decision(
        classification=READY_FOR_PROOF,
        blockers=[],
        secondary_warnings=[],
        broker_lease_warning=broker_lease_warning,
        phase1_session_reason=phase1_session_reason,
    )


def _decision(
    *,
    classification: str,
    blockers: Sequence[Mapping[str, Any]],
    secondary_warnings: Sequence[Mapping[str, Any]],
    broker_lease_warning: Mapping[str, Any] | None,
    phase1_session_reason: str | None,
) -> dict[str, Any]:
    blocker_list = [dict(blocker) for blocker in blockers]
    warning_list = [dict(warning) for warning in secondary_warnings]
    return {
        "classification": classification,
        "primary_blocker": blocker_list[0] if blocker_list else None,
        "secondary_warnings": warning_list,
        "broker_lease_warning": dict(broker_lease_warning) if broker_lease_warning else None,
        "phase1_session_reason": phase1_session_reason,
        "blockers": blocker_list,
    }


def _required_phase1_checks(
    *,
    rows: Sequence[Mapping[str, Any]],
    symbols: Sequence[str],
    timeframes: Sequence[str],
) -> list[dict[str, Any]]:
    rows_by_symbol = {str(row.get("symbol") or "").upper(): row for row in rows}
    checks: list[dict[str, Any]] = []
    for symbol in symbols:
        row = _mapping(rows_by_symbol.get(symbol.upper()))
        candle_checks = _mapping(row.get("candle_checks"))
        for timeframe in timeframes:
            check = _mapping(candle_checks.get(timeframe))
            if not check:
                check = {
                    "ready": False,
                    "reason": "PHASE1_RUNTIME_CANDLE_CHECK_MISSING",
                    "path": None,
                }
            checks.append(
                {
                    "symbol": symbol.upper(),
                    "timeframe": timeframe,
                    "ready": check.get("ready") is True,
                    "reason": check.get("reason") or "UNKNOWN",
                    "path": check.get("path"),
                    "generated_at": check.get("generated_at"),
                    "age_seconds": check.get("age_seconds"),
                    "freshness_seconds": check.get("freshness_seconds"),
                    "market_session": check.get("market_session") or {},
                }
            )
    return checks


def _phase1_blockers(checks: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    for check in checks:
        symbol = str(check.get("symbol") or "")
        timeframe = str(check.get("timeframe") or "")
        reason = str(check.get("reason") or "UNKNOWN")
        blockers.append(
            {
                "code": f"phase1_{symbol.lower()}_{timeframe}_not_ready",
                "detail": f"Phase-1 {symbol} {timeframe} runtime candles are not proof-ready: {reason}.",
                "symbol": symbol,
                "timeframe": timeframe,
                "reason": reason,
                "path": str(check.get("path") or ""),
            }
        )
    return blockers


def _secondary_warnings(
    *,
    shared_blockers: Sequence[Mapping[str, Any]],
    phase1_blockers: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    for blocker in shared_blockers:
        warnings.append(
            {
                **dict(blocker),
                "warning_type": "shared_truth_evidence",
            }
        )
    for blocker in phase1_blockers:
        warnings.append(
            {
                **dict(blocker),
                "warning_type": "phase1_evidence",
            }
        )
    return warnings


def _broker_lease_warning(
    *,
    classifications: Mapping[str, Any],
    blockers: Sequence[Mapping[str, Any]],
) -> dict[str, Any] | None:
    lease_state = str(classifications.get("Broker Truth Lease") or "")
    if lease_state == "ACTIVE":
        return None
    blocker = next((dict(row) for row in blockers if str(row.get("code") or "") == "broker_truth_lease_not_clean_for_runtime_start"), {})
    if not lease_state and not blocker:
        return None
    return {
        "lease_state": lease_state or blocker.get("observed"),
        "code": blocker.get("code") or "broker_truth_lease_not_active",
        "detail": blocker.get("detail") or f"Broker Truth Lease is {lease_state}.",
        "primary_blocker": False,
    }


def _is_degraded_broker_lease_blocker(blocker: Mapping[str, Any]) -> bool:
    observed = str(blocker.get("observed") or blocker.get("detail") or "")
    return (
        str(blocker.get("code") or "") == "broker_truth_lease_not_clean_for_runtime_start"
        and "ACTIVE_DEGRADED_REFRESH_FAILING" in observed
    )


def _phase1_session_reason(*, phase1_checks: Sequence[Mapping[str, Any]]) -> str | None:
    for check in phase1_checks:
        session = _mapping(check.get("market_session"))
        reason = str(session.get("reason") or "")
        if reason:
            return reason
    return None


def _broker_state_unsafe(blockers: Sequence[Mapping[str, Any]]) -> bool:
    broker_codes = {"live_money_eligible_not_false"}
    return any(str(blocker.get("code") or "") in broker_codes for blocker in blockers)


def _print_summary(payload: Mapping[str, Any]) -> None:
    print("Track B PAPER Proof Readiness")
    print(f"generated_at: {payload.get('generated_at')}")
    print(f"classification: {payload.get('classification')}")
    print("")
    print("Shared Truth:")
    for service, classification in _mapping(payload.get("shared_truth_classifications")).items():
        print(f"- {service}: {classification}")
    print("")
    print("Phase-1 MGC/MNQ candles:")
    for check in _list(payload.get("phase1_required_checks")):
        print(
            f"- {check.get('symbol')} {check.get('timeframe')}: "
            f"{'READY' if check.get('ready') else check.get('reason')}"
        )
    blockers = _list(payload.get("blockers"))
    if blockers:
        print("")
        print("Blockers:")
        for blocker in blockers:
            print(f"- {blocker.get('code')}: {blocker.get('detail')}")


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    return _ensure_utc(datetime.fromisoformat(text))


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


if __name__ == "__main__":
    raise SystemExit(main())
