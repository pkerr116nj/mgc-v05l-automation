"""Episode-centric Track B advisory layer backfill pilot.

This module is research/offline only. It reads explicit historical candidate
and bar partitions, writes bounded advisory Parquet rows, and never imports or
invokes broker, runtime, strategy, lane execution, order, or lifecycle writers.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.research.trend_participation.storage import materialize_parquet_dataset, write_storage_manifest


SCHEMA_VERSION = "track_b_advisory_layer_backfill_pilot_v1"
BUILD_NAME = "track_b_advisory_layer_backfill_pilot"
STRATEGY_FAMILY = "exact_baseline"
EXIT_PROFILE = "fixed_36b"
DERIVED_TIMEFRAME = "5m"
BASE_TIMEFRAME = "1m"
BASE_DATASET_NAME = "base_1m"
ADVISORY_DATASET_NAME = "advisory_layer_backfill"
DEFAULT_WINDOW_BARS = 48
DEFAULT_DEDUPE_BARS = 12
LAYERS = (
    "lifecycle_awareness",
    "participation_pressure",
    "regime_session",
    "sizing_position_management",
)
SAFETY_FLAGS = {
    "strategy_authority": False,
    "broker_state_mutated": False,
    "submit_attempted": False,
    "order_intent_created": False,
    "lifecycle_mutated": False,
    "runtime_trade_eligible": False,
}


@dataclass(frozen=True)
class BackfillPilotConfig:
    mode: str
    symbols: tuple[str, ...]
    year: int | None
    quarter: str | None
    start: datetime | None
    end: datetime | None
    candidate_root: Path
    base_1m_root: Path
    output_root: Path
    window_bars: int
    dedupe_bars: int
    expected_episodes: int | None
    expected_episode_counts_json: Path | None
    no_delete: bool
    skip_existing: bool


@dataclass(frozen=True)
class PartitionSpec:
    symbol: str
    year: int
    quarter: str

    @property
    def shard_id(self) -> str:
        return f"{self.year}{self.quarter}"

    @property
    def partition_id(self) -> str:
        return f"{self.symbol}_{self.shard_id}"


@dataclass(frozen=True)
class Episode:
    episode_id: str
    candidate_id: str
    symbol: str
    side: str
    decision_ts: datetime
    entry_price: float
    source_family: str
    lane_id: str
    candidate_path: Path
    derived_bars_path: Path
    base_1m_partition_path: Path
    base_1m_manifest_path: Path
    base_1m_manifest: Mapping[str, Any]
    window_bars: tuple[Mapping[str, Any], ...]
    spillover_partitions_read: tuple[str, ...]


def main(argv: Sequence[str] | None = None) -> int:
    config = _parse_args(argv)
    result = run_backfill_pilot(config)
    print(json.dumps(_summary(result), indent=2, sort_keys=True))
    return 0


def run_backfill_pilot(config: BackfillPilotConfig) -> dict[str, Any]:
    _validate_config(config)
    started = time.monotonic()
    run_id = f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}__{BUILD_NAME}"
    build_created_at = datetime.now(UTC)
    source_git_commit = _source_git_commit()
    planned_partitions = _planned_partitions(config)
    expected_counts = _expected_episode_counts(config)
    partition_results: list[dict[str, Any]] = []
    failed_partitions: list[dict[str, Any]] = []

    for spec in planned_partitions:
        candidate_path = _candidate_path(config, spec)
        derived_bars_path = _derived_bars_path(config, spec)
        base_partition_path = _base_1m_partition_path(config, spec)
        base_manifest_path = base_partition_path.parent / "partition_manifest.json"
        base_manifest = _load_base_1m_manifest(base_partition_path, base_manifest_path)
        candidates = _load_parquet_rows(candidate_path)
        bars = _load_parquet_rows(derived_bars_path)
        deduped = _dedupe_candidates(candidates, dedupe_bars=config.dedupe_bars)
        episodes, spillover_summary = _build_episodes(
            config=config,
            spec=spec,
            symbol=spec.symbol,
            candidate_rows=deduped,
            bar_rows=bars,
            window_bars=config.window_bars,
            candidate_path=candidate_path,
            derived_bars_path=derived_bars_path,
            base_partition_path=base_partition_path,
            base_manifest_path=base_manifest_path,
            base_manifest=base_manifest,
        )
        advisory_rows_by_layer, episode_rows = _materialize_partition_rows(
            config=config,
            spec=spec,
            episodes=episodes,
            run_id=run_id,
            build_created_at=build_created_at,
            source_git_commit=source_git_commit,
        )
        output_paths = _partition_output_paths(config, spec)
        partition_validation = _partition_validation_summary(
            config=config,
            run_id=run_id,
            spec=spec,
            episodes=episodes,
            advisory_rows_by_layer=advisory_rows_by_layer,
            raw_candidate_count=len(candidates),
            skipped_by_dedupe=len(candidates) - len(deduped),
        )
        if partition_validation["failure_reasons"]:
            failed_partitions.append(
                {
                    "partition_id": spec.partition_id,
                    "failure_reasons": partition_validation["failure_reasons"],
                }
            )
        partition_manifest = _partition_manifest(
            config=config,
            run_id=run_id,
            spec=spec,
            build_created_at=build_created_at,
            source_git_commit=source_git_commit,
            raw_candidate_count=len(candidates),
            skipped_by_dedupe=len(candidates) - len(deduped),
            episode_count=len(episodes),
            advisory_rows_by_layer=advisory_rows_by_layer,
            output_paths=output_paths,
            source_inputs={
                "candidate_path": str(candidate_path),
                "derived_bars_path": str(derived_bars_path),
                "base_1m_partition_path": str(base_partition_path),
                "base_1m_manifest_path": str(base_manifest_path),
            },
            spillover_summary=spillover_summary,
            validation_summary=partition_validation,
        )
        partition_action = "write"
        existing_validation: dict[str, Any] | None = None
        if config.mode == "apply" and any(path.exists() for path in output_paths.values()):
            if config.skip_existing:
                existing_validation = _validate_existing_partition(
                    expected_manifest=partition_manifest,
                    output_paths=output_paths,
                )
                partition_action = "skip_existing"
            else:
                failed_partitions.append(
                    {
                        "partition_id": spec.partition_id,
                        "failure_reasons": ["REFUSING_TO_OVERWRITE_EXISTING_PARTITION"],
                    }
                )
                partition_action = "failed_existing"
        partition_results.append(
            {
                "spec": spec,
                "raw_candidate_count": len(candidates),
                "skipped_by_dedupe": len(candidates) - len(deduped),
                "episode_count": len(episodes),
                "advisory_rows_by_layer": advisory_rows_by_layer,
                "episode_rows": episode_rows,
                "output_paths": output_paths,
                "partition_manifest": partition_manifest,
                "partition_validation": partition_validation,
                "partition_action": partition_action,
                "existing_validation": existing_validation,
                "spillover_summary": spillover_summary,
            }
        )

    total_episodes = sum(int(result["episode_count"]) for result in partition_results)
    quarter_counts = _quarter_episode_counts(partition_results)
    reconciliation_failures = _episode_reconciliation_failures(
        config=config,
        expected_counts=expected_counts,
        observed_quarter_counts=quarter_counts,
        total_episodes=total_episodes,
    )
    if reconciliation_failures:
        raise SystemExit(
            json.dumps(
                {
                    "compatibility_uncertain": True,
                    "failure_reasons": reconciliation_failures,
                    "expected_episode_counts": expected_counts,
                    "observed_quarter_episode_counts": quarter_counts,
                    "observed_total_episodes": total_episodes,
                },
                sort_keys=True,
            )
        )

    if failed_partitions:
        raise SystemExit(json.dumps({"failed_partitions": failed_partitions}, sort_keys=True))

    runtime_seconds = round(time.monotonic() - started, 6)
    run_output_paths = _run_output_paths(config, run_id=run_id)
    validation_summary = _run_validation_summary(
        config=config,
        run_id=run_id,
        partition_results=partition_results,
        expected_counts=expected_counts,
        quarter_counts=quarter_counts,
        runtime_seconds=runtime_seconds,
    )
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "build_name": BUILD_NAME,
        "run_id": run_id,
        "mode": config.mode,
        "build_created_at": build_created_at.isoformat(),
        "source_git_commit": source_git_commit,
        "strategy_family": STRATEGY_FAMILY,
        "exit_profile": EXIT_PROFILE,
        "symbols": list(config.symbols),
        "year": config.year,
        "quarter": config.quarter,
        "start": config.start.isoformat() if config.start else None,
        "end": config.end.isoformat() if config.end else None,
        "episode_window_bars": config.window_bars,
        "dedupe_bars": config.dedupe_bars,
        "episode_centric": True,
        "global_bar_backfill": False,
        "planned_partitions": [_partition_summary(result) for result in partition_results],
        "written_partitions": [_partition_summary(result) for result in partition_results if result["partition_action"] == "write"],
        "skipped_partitions": [
            _partition_summary(result) for result in partition_results if result["partition_action"] == "skip_existing"
        ],
        "failed_partitions": failed_partitions,
        "raw_candidate_counts": _counts_by_partition(partition_results, "raw_candidate_count"),
        "skipped_by_dedupe": _counts_by_partition(partition_results, "skipped_by_dedupe"),
        "episode_count": total_episodes,
        "advisory_row_count": sum(_partition_advisory_row_count(result) for result in partition_results),
        "advisory_row_count_by_layer": _run_layer_counts(partition_results),
        "observed_quarter_episode_counts": quarter_counts,
        "expected_episode_counts": expected_counts,
        "spillover_partitions_read": sorted(
            {
                partition
                for result in partition_results
                for partition in result["spillover_summary"].get("spillover_partitions_read", [])
            }
        ),
        "episodes_requiring_spillover": [
            episode
            for result in partition_results
            for episode in result["spillover_summary"].get("episodes_requiring_spillover", [])
        ],
        "missing_spillover_failures": [
            failure
            for result in partition_results
            for failure in result["spillover_summary"].get("missing_spillover_failures", [])
        ],
        "runtime_seconds": runtime_seconds,
        "output_paths": {key: str(value) for key, value in run_output_paths.items()},
        "validation_summary_path": str(run_output_paths["validation_summary"]),
        "no_delete": config.no_delete,
        "skip_existing": config.skip_existing,
        "wrote_outputs": config.mode == "apply",
        "deleted_outputs": False,
        "source_mutated": False,
        "runtime_activity": False,
        "strategy_behavior_changed": False,
        **SAFETY_FLAGS,
    }

    if config.mode == "apply":
        _assert_run_outputs_do_not_exist(run_output_paths)
        for result in partition_results:
            if result["partition_action"] == "skip_existing":
                continue
            _write_partition_outputs(result)
        write_storage_manifest(run_output_paths["manifest"], manifest)
        write_storage_manifest(run_output_paths["validation_summary"], validation_summary)

    return {
        **manifest,
        "validation_summary": validation_summary,
    }


def _parse_args(argv: Sequence[str] | None) -> BackfillPilotConfig:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", required=True, choices=("dry-run", "apply"))
    parser.add_argument("--symbols", required=True, help="Comma-separated symbols, for example GC,MGC.")
    parser.add_argument("--year", type=int)
    parser.add_argument("--quarter", choices=("Q1", "Q2", "Q3", "Q4"))
    parser.add_argument("--start", help="Inclusive ISO timestamp/date for multi-quarter planning.")
    parser.add_argument("--end", help="Inclusive ISO timestamp/date for multi-quarter planning.")
    parser.add_argument("--candidate-root", required=True, type=Path)
    parser.add_argument("--base-1m-root", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--window-bars", type=int, default=DEFAULT_WINDOW_BARS)
    parser.add_argument("--dedupe-bars", type=int, default=DEFAULT_DEDUPE_BARS)
    parser.add_argument("--expected-episodes", type=int)
    parser.add_argument("--expected-episode-counts-json", type=Path)
    parser.add_argument("--no-delete", action="store_true")
    parser.add_argument("--skip-existing", action="store_true")
    args = parser.parse_args(argv)
    return BackfillPilotConfig(
        mode=str(args.mode),
        symbols=tuple(_parse_symbols(str(args.symbols))),
        year=args.year,
        quarter=str(args.quarter) if args.quarter else None,
        start=_parse_optional_timestamp(args.start),
        end=_parse_optional_timestamp(args.end),
        candidate_root=args.candidate_root,
        base_1m_root=args.base_1m_root,
        output_root=args.output_root,
        window_bars=int(args.window_bars),
        dedupe_bars=int(args.dedupe_bars),
        expected_episodes=args.expected_episodes,
        expected_episode_counts_json=args.expected_episode_counts_json,
        no_delete=bool(args.no_delete),
        skip_existing=bool(args.skip_existing),
    )


def _validate_config(config: BackfillPilotConfig) -> None:
    if not config.no_delete:
        raise SystemExit("--no-delete is required for advisory backfill pilot")
    if config.window_bars < 1:
        raise SystemExit("--window-bars must be positive")
    if config.dedupe_bars < 1:
        raise SystemExit("--dedupe-bars must be positive")
    if (config.year is None) != (config.quarter is None):
        raise SystemExit("--year and --quarter must be supplied together")
    if (config.start is None) != (config.end is None):
        raise SystemExit("--start and --end must be supplied together")
    if config.year is None and config.start is None:
        raise SystemExit("supply either --year/--quarter or --start/--end")
    if config.year is not None and config.start is not None:
        raise SystemExit("use either --year/--quarter or --start/--end, not both")
    if config.start is not None and config.end is not None and config.end < config.start:
        raise SystemExit("--end must be >= --start")
    if not config.candidate_root.exists():
        raise SystemExit(f"candidate root not found: {config.candidate_root}")
    if not config.base_1m_root.exists():
        raise SystemExit(f"base 1m root not found: {config.base_1m_root}")


def _candidate_path(config: BackfillPilotConfig, spec: PartitionSpec) -> Path:
    return (
        config.candidate_root
        / spec.symbol
        / spec.shard_id
        / "datasets"
        / "lane_candidates"
        / f"symbol={spec.symbol}"
        / f"year={spec.year}"
        / f"shard_id={spec.shard_id}"
        / "candidates.parquet"
    )


def _derived_bars_path(config: BackfillPilotConfig, spec: PartitionSpec) -> Path:
    return (
        config.candidate_root
        / spec.symbol
        / spec.shard_id
        / "datasets"
        / "derived_bars_5m"
        / f"symbol={spec.symbol}"
        / f"year={spec.year}"
        / f"shard_id={spec.shard_id}"
        / "bars.parquet"
    )


def _base_1m_partition_path(config: BackfillPilotConfig, spec: PartitionSpec) -> Path:
    return config.base_1m_root / spec.symbol / str(spec.year) / spec.quarter / "bars.parquet"


def _next_partition_spec(spec: PartitionSpec) -> PartitionSpec:
    quarter_number = int(spec.quarter.removeprefix("Q"))
    if quarter_number == 4:
        return PartitionSpec(symbol=spec.symbol, year=spec.year + 1, quarter="Q1")
    return PartitionSpec(symbol=spec.symbol, year=spec.year, quarter=f"Q{quarter_number + 1}")


def _load_base_1m_manifest(partition_path: Path, manifest_path: Path) -> Mapping[str, Any]:
    if not partition_path.exists():
        raise SystemExit(f"required base 1m Parquet hot-cache partition not found: {partition_path}")
    if not manifest_path.exists():
        raise SystemExit(f"required base 1m partition manifest not found: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("timeframe") != BASE_TIMEFRAME:
        raise SystemExit(f"base 1m manifest timeframe mismatch: {manifest_path}")
    if manifest.get("data_source") != "historical_1m_canonical":
        raise SystemExit(f"base 1m manifest data_source mismatch: {manifest_path}")
    return manifest


def _load_parquet_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise SystemExit(f"required Parquet input not found: {path}")
    import pyarrow.parquet as pq

    return [dict(row) for row in pq.ParquetFile(path).read().to_pylist()]


def _dedupe_candidates(rows: Sequence[Mapping[str, Any]], *, dedupe_bars: int) -> list[Mapping[str, Any]]:
    cooldown = timedelta(minutes=dedupe_bars * 5)
    deduped: list[Mapping[str, Any]] = []
    last_kept: datetime | None = None
    for row in sorted(rows, key=lambda item: _coerce_ts(item["decision_ts"])):
        decision_ts = _coerce_ts(row["decision_ts"])
        if last_kept is None or decision_ts - last_kept >= cooldown:
            deduped.append(row)
            last_kept = decision_ts
    return deduped


def _build_episodes(
    *,
    config: BackfillPilotConfig,
    spec: PartitionSpec,
    symbol: str,
    candidate_rows: Sequence[Mapping[str, Any]],
    bar_rows: Sequence[Mapping[str, Any]],
    window_bars: int,
    candidate_path: Path,
    derived_bars_path: Path,
    base_partition_path: Path,
    base_manifest_path: Path,
    base_manifest: Mapping[str, Any],
) -> tuple[list[Episode], dict[str, Any]]:
    sorted_bars = sorted(bar_rows, key=lambda item: _coerce_ts(item["bar_ts"]))
    partition_ids_by_ts: dict[datetime, str] = {_coerce_ts(row["bar_ts"]): spec.partition_id for row in sorted_bars}
    spillover_rows_by_partition: dict[str, list[dict[str, Any]]] = {}
    episodes: list[Episode] = []
    episodes_requiring_spillover: list[str] = []
    missing_spillover_failures: list[dict[str, Any]] = []
    for sequence, candidate in enumerate(candidate_rows):
        bar_index = {_coerce_ts(row["bar_ts"]): index for index, row in enumerate(sorted_bars)}
        decision_ts = _coerce_ts(candidate["decision_ts"])
        if decision_ts not in bar_index:
            raise SystemExit(f"candidate decision_ts missing from derived bars: {symbol} {decision_ts.isoformat()}")
        start_index = bar_index[decision_ts]
        stop_index = start_index + window_bars + 1
        window = tuple(sorted_bars[start_index:stop_index])
        if len(window) != window_bars + 1:
            next_spec = _next_partition_spec(spec)
            next_path = _derived_bars_path(config, next_spec)
            if not next_path.exists():
                failure = {
                    "candidate_id": str(candidate.get("candidate_id")),
                    "entry_partition": spec.partition_id,
                    "required_spillover_partition": next_spec.partition_id,
                    "missing_path": str(next_path),
                    "expected_window_bars": window_bars + 1,
                    "observed_window_bars": len(window),
                    "failure_reason": "BOUNDARY_SPILLOVER_MISSING",
                }
                missing_spillover_failures.append(failure)
                raise SystemExit(json.dumps({"boundary_spillover_missing": failure}, sort_keys=True))
            if next_spec.partition_id not in spillover_rows_by_partition:
                next_rows = _load_parquet_rows(next_path)
                spillover_rows_by_partition[next_spec.partition_id] = next_rows
                sorted_bars = sorted([*sorted_bars, *next_rows], key=lambda item: _coerce_ts(item["bar_ts"]))
                for row in next_rows:
                    partition_ids_by_ts[_coerce_ts(row["bar_ts"])] = next_spec.partition_id
            bar_index = {_coerce_ts(row["bar_ts"]): index for index, row in enumerate(sorted_bars)}
            start_index = bar_index[decision_ts]
            window = tuple(sorted_bars[start_index:stop_index])
            if len(window) != window_bars + 1:
                failure = {
                    "candidate_id": str(candidate.get("candidate_id")),
                    "entry_partition": spec.partition_id,
                    "required_spillover_partition": next_spec.partition_id,
                    "expected_window_bars": window_bars + 1,
                    "observed_window_bars": len(window),
                    "failure_reason": "BOUNDARY_SPILLOVER_INSUFFICIENT",
                }
                missing_spillover_failures.append(failure)
                raise SystemExit(json.dumps({"boundary_spillover_missing": failure}, sort_keys=True))
            episodes_requiring_spillover.append(str(candidate.get("candidate_id")))
        spillover_partitions_read = tuple(
            sorted(
                {
                    partition_ids_by_ts[_coerce_ts(row["bar_ts"])]
                    for row in window
                    if partition_ids_by_ts.get(_coerce_ts(row["bar_ts"])) != spec.partition_id
                }
            )
        )
        side = str(candidate.get("side") or "UNKNOWN").upper()
        entry_bar = window[0]
        entry_price = float(entry_bar["close"])
        episode_id = _stable_id(symbol, str(candidate.get("candidate_id")), decision_ts.isoformat(), str(sequence))
        episodes.append(
            Episode(
                episode_id=episode_id,
                candidate_id=str(candidate.get("candidate_id")),
                symbol=symbol,
                side=side,
                decision_ts=decision_ts,
                entry_price=entry_price,
                source_family=str(candidate.get("family") or candidate.get("source_event_family") or "UNKNOWN"),
                lane_id=str(candidate.get("lane_id") or ""),
                candidate_path=candidate_path,
                derived_bars_path=derived_bars_path,
                base_1m_partition_path=base_partition_path,
                base_1m_manifest_path=base_manifest_path,
                base_1m_manifest=base_manifest,
                window_bars=window,
                spillover_partitions_read=spillover_partitions_read,
            )
        )
    return episodes, {
        "spillover_partitions_read": sorted(spillover_rows_by_partition),
        "episodes_requiring_spillover": episodes_requiring_spillover,
        "missing_spillover_failures": missing_spillover_failures,
    }


def _materialize_partition_rows(
    *,
    config: BackfillPilotConfig,
    spec: PartitionSpec,
    episodes: Sequence[Episode],
    run_id: str,
    build_created_at: datetime,
    source_git_commit: str,
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
    advisory_rows_by_layer: dict[str, list[dict[str, Any]]] = {layer: [] for layer in LAYERS}
    episode_rows: list[dict[str, Any]] = []
    for sequence, episode in enumerate(sorted(episodes, key=lambda item: (item.symbol, item.decision_ts))):
        episode_rows.append(_episode_index_row(episode, sequence=sequence, config=config, spec=spec, run_id=run_id))
        for offset, bar in enumerate(episode.window_bars):
            layer_context = _classify_window(episode=episode, offset=offset)
            for layer in LAYERS:
                advisory_rows_by_layer[layer].append(
                    _advisory_row(
                        episode=episode,
                        bar=bar,
                        offset=offset,
                        layer=layer,
                        layer_context=layer_context,
                        config=config,
                        spec=spec,
                        run_id=run_id,
                        build_created_at=build_created_at,
                        source_git_commit=source_git_commit,
                    )
                )
    return advisory_rows_by_layer, episode_rows


def _classify_window(*, episode: Episode, offset: int) -> dict[str, Any]:
    elapsed = episode.window_bars[: offset + 1]
    current = elapsed[-1]
    side_sign = -1.0 if episode.side == "SHORT" else 1.0
    progress = (float(current["close"]) - episode.entry_price) * side_sign
    favorable_moves = [(float(bar["high"]) - episode.entry_price) * side_sign for bar in elapsed]
    adverse_moves = [(float(bar["low"]) - episode.entry_price) * side_sign for bar in elapsed]
    if episode.side == "SHORT":
        favorable_moves = [(episode.entry_price - float(bar["low"])) for bar in elapsed]
        adverse_moves = [(episode.entry_price - float(bar["high"])) for bar in elapsed]
    mfe = max(favorable_moves)
    mae = min(adverse_moves)
    recent = elapsed[-min(6, len(elapsed)) :]
    ranges = [float(bar["high"]) - float(bar["low"]) for bar in recent]
    bodies = [abs(float(bar["close"]) - float(bar["open"])) for bar in recent]
    signed_bodies = [(float(bar["close"]) - float(bar["open"])) * side_sign for bar in recent]
    avg_range = sum(ranges) / len(ranges)
    avg_body = sum(bodies) / len(bodies)
    directional_score = sum(1 for value in signed_bodies if value > 0) / len(signed_bodies)
    giveback = mfe - progress

    if offset <= 2:
        lifecycle_state = "NEWLY_OPENED"
    elif progress > 0 and mfe >= avg_range * 2.0 and giveback <= max(avg_range, 0.01):
        lifecycle_state = "FAVORABLE_EXPANSION"
    elif progress > 0 and giveback > avg_range:
        lifecycle_state = "HEALTHY_PULLBACK"
    elif progress <= 0 and abs(mae) > max(mfe, avg_range):
        lifecycle_state = "ADVERSE_DOMINANCE"
    elif offset >= 12 and abs(progress) <= max(avg_range * 0.35, 0.01):
        lifecycle_state = "STALLED"
    elif mfe > avg_range and giveback > avg_range * 1.5:
        lifecycle_state = "DECAYING"
    else:
        lifecycle_state = "WORKING_IN_FAVOR" if progress > 0 else "STALLED"

    if directional_score >= 0.67 and progress >= 0:
        pressure_state = "PERSISTENT_BULLISH_PRESSURE" if episode.side != "SHORT" else "PERSISTENT_BEARISH_PRESSURE"
        directional_bias = "WITH_ENTRY"
    elif directional_score <= 0.33 and progress < 0:
        pressure_state = "PARTICIPATION_COLLAPSE"
        directional_bias = "AGAINST_ENTRY"
    elif lifecycle_state == "DECAYING":
        pressure_state = "IMPULSE_DECAYING"
        directional_bias = "MIXED"
    else:
        pressure_state = "CHOP_BALANCED"
        directional_bias = "BALANCED"

    if avg_range >= max(0.01, avg_body * 2.8):
        regime_state = "REGIME_EXPANSION"
        volatility_range_state = "RANGE_EXPANDED"
    elif avg_body <= max(avg_range * 0.20, 0.01):
        regime_state = "REGIME_COMPRESSION"
        volatility_range_state = "RANGE_COMPRESSED"
    elif 0.40 <= directional_score <= 0.60:
        regime_state = "REGIME_CHOP_BALANCED"
        volatility_range_state = "RANGE_NORMAL"
    else:
        regime_state = "REGIME_TRENDING"
        volatility_range_state = "RANGE_NORMAL"

    hostile = lifecycle_state in {"DECAYING", "ADVERSE_DOMINANCE"} or pressure_state == "PARTICIPATION_COLLAPSE"
    sizing_initial = "BASE_UNIT" if offset == 0 else "UNKNOWN"
    sizing_in_position = "REDUCE_PARTIAL" if hostile else "HOLD_FULL"

    return {
        "lifecycle_awareness_state": lifecycle_state,
        "pressure_state": pressure_state,
        "directional_bias": directional_bias,
        "market_regime_state": regime_state,
        "volatility_range_state": volatility_range_state,
        "trend_chop_state": "TRENDING" if regime_state == "REGIME_TRENDING" else "CHOP_OR_RANGE",
        "liquidity_state": "LIQUIDITY_NORMAL",
        "directional_context": directional_bias,
        "initial_size_context": sizing_initial,
        "in_position_size_context": sizing_in_position,
        "add_size_context": "NOT_ALLOWED_V1",
        "mfe": round(mfe, 6),
        "mae": round(mae, 6),
        "unrealized_progress": round(progress, 6),
        "confidence": 0.72 if not hostile else 0.56,
        "state_reasons": ["EPISODE_CENTRIC_PILOT_HEURISTIC", "EXACT_BASELINE_SOURCE_RECONCILED"],
        "failure_reasons": [],
        "warning_reasons": ["PILOT_BACKFILL_NOT_RUNTIME_TRUTH"],
    }


def _advisory_row(
    *,
    episode: Episode,
    bar: Mapping[str, Any],
    offset: int,
    layer: str,
    layer_context: Mapping[str, Any],
    config: BackfillPilotConfig,
    spec: PartitionSpec,
    run_id: str,
    build_created_at: datetime,
    source_git_commit: str,
) -> dict[str, Any]:
    evaluation_ts = _coerce_ts(bar["bar_ts"])
    row = {
        "schema_version": SCHEMA_VERSION,
        "layer_name": layer,
        "layer_contract": _layer_contract(layer),
        "build_id": run_id,
        "build_created_at": build_created_at.isoformat(),
        "source_git_commit": source_git_commit,
        "evaluator_module": "mgc_v05l.app.track_b_advisory_layer_backfill_pilot",
        "evaluator_version": SCHEMA_VERSION,
        "strategy_family": STRATEGY_FAMILY,
        "strategy_id": episode.lane_id,
        "exit_profile": EXIT_PROFILE,
        "instrument": episode.symbol,
        "root_symbol": episode.symbol,
        "contract_symbol": episode.symbol,
        "timeframe": DERIVED_TIMEFRAME,
        "base_timeframe": BASE_TIMEFRAME,
        "derived_timeframe": DERIVED_TIMEFRAME,
        "session_bucket": "HISTORICAL_REPLAY",
        "episode_id": episode.episode_id,
        "candidate_id": episode.candidate_id,
        "position_id": "",
        "replay_window_id": f"{episode.episode_id}:{config.window_bars}b",
        "entry_side": episode.side,
        "entry_price": round(episode.entry_price, 6),
        "entry_timestamp": episode.decision_ts.isoformat(),
        "evaluation_timestamp": evaluation_ts.isoformat(),
        "window_start_timestamp": episode.decision_ts.isoformat(),
        "window_end_timestamp": _coerce_ts(episode.window_bars[-1]["bar_ts"]).isoformat(),
        "bars_since_entry": offset,
        "window_bar_index": offset,
        "lifecycle_window_bars": config.window_bars,
        "input_source_category": "HISTORICAL_RESEARCH_REPLAY",
        "input_dataset_id": "exact_baseline_lane_candidates_derived_5m",
        "input_dataset_version": spec.shard_id,
        "input_parquet_partition_path": str(episode.derived_bars_path),
        "input_partition_manifest_path": str(episode.base_1m_manifest_path),
        "source_sqlite_path": str(episode.base_1m_manifest.get("source_sqlite_path", "")),
        "input_row_hash": _stable_id(episode.candidate_id, evaluation_ts.isoformat(), layer),
        "source_window_hash": _source_window_hash(episode.window_bars),
        "provenance_status": "PROVENANCE_COMPLETE",
        "freshness_status": "HISTORICAL_AS_OF_REPLAY",
        "runtime_eligible": False,
        "confidence": layer_context["confidence"],
        "failure_reasons": layer_context["failure_reasons"],
        "warning_reasons": layer_context["warning_reasons"],
        **SAFETY_FLAGS,
    }
    row.update(_layer_fields(layer, layer_context))
    return row


def _layer_fields(layer: str, context: Mapping[str, Any]) -> dict[str, Any]:
    if layer == "lifecycle_awareness":
        return {
            "lifecycle_awareness_state": context["lifecycle_awareness_state"],
            "hold_quality_context": "SUPPORTIVE" if context["unrealized_progress"] >= 0 else "WEAKENING",
            "exit_urgency_context": "NORMAL" if context["unrealized_progress"] >= 0 else "ELEVATED",
            "reduce_size_context": "NONE" if context["unrealized_progress"] >= 0 else "ADVISORY_REDUCE_CONTEXT",
            "add_size_context": "NOT_ALLOWED_V1",
            "patience_context": "PATIENT" if context["unrealized_progress"] >= 0 else "LOW_PATIENCE",
            "state_reasons": context["state_reasons"],
            "mfe": context["mfe"],
            "mae": context["mae"],
            "unrealized_progress": context["unrealized_progress"],
        }
    if layer == "participation_pressure":
        return {
            "pressure_state": context["pressure_state"],
            "directional_bias": context["directional_bias"],
            "hold_quality_context": "SUPPORTIVE" if context["directional_bias"] == "WITH_ENTRY" else "NEUTRAL_OR_WEAKENING",
            "exit_urgency_context": "NORMAL" if context["directional_bias"] != "AGAINST_ENTRY" else "ELEVATED",
            "pressure_confidence": context["confidence"],
            "pressure_reasons": context["state_reasons"],
        }
    if layer == "regime_session":
        return {
            "regime_session_context_v1": "track_b_regime_session_context_v1",
            "market_regime_state": context["market_regime_state"],
            "volatility_range_state": context["volatility_range_state"],
            "trend_chop_state": context["trend_chop_state"],
            "liquidity_state": context["liquidity_state"],
            "directional_context": context["directional_context"],
            "regime_reasons": context["state_reasons"],
        }
    if layer == "sizing_position_management":
        return {
            "initial_size_context": context["initial_size_context"],
            "in_position_size_context": context["in_position_size_context"],
            "add_size_context": context["add_size_context"],
            "sizing_reasons": ["ADD_SIZE_DISABLED_V1", *context["state_reasons"]],
        }
    raise ValueError(f"unsupported layer: {layer}")


def _episode_index_row(
    episode: Episode,
    *,
    sequence: int,
    config: BackfillPilotConfig,
    spec: PartitionSpec,
    run_id: str,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "build_id": run_id,
        "episode_id": episode.episode_id,
        "episode_sequence": sequence,
        "strategy_family": STRATEGY_FAMILY,
        "instrument": episode.symbol,
        "entry_timestamp": episode.decision_ts.isoformat(),
        "entry_side": episode.side,
        "entry_price": round(episode.entry_price, 6),
        "fixed_exit_timestamp_36b": _coerce_ts(episode.window_bars[36]["bar_ts"]).isoformat()
        if len(episode.window_bars) > 36
        else "",
        "episode_year": spec.year,
        "source_candidate_id": episode.candidate_id,
        "source_trade_id": "",
        "source_backtest_run_id": f"exact_baseline_{spec.shard_id}",
        "source_parquet_partitions": [str(episode.candidate_path), str(episode.derived_bars_path)],
        "spillover_partitions_read": list(episode.spillover_partitions_read),
        "episode_status": "VALID_WINDOW",
        "episode_failure_reasons": [],
        "runtime_eligible": False,
        **SAFETY_FLAGS,
    }


def _partition_output_paths(config: BackfillPilotConfig, spec: PartitionSpec) -> dict[str, Path]:
    root = config.output_root
    partition_root = (
        root
        / "partitions"
        / f"strategy_family={STRATEGY_FAMILY}"
        / f"instrument={spec.symbol}"
        / f"year={spec.year}"
        / spec.quarter
    )
    paths: dict[str, Path] = {
        "partition_manifest": partition_root / "partition_manifest.json",
        "partition_validation_summary": partition_root / "validation_summary.json",
        "episodes_parquet": (
            root
            / "episodes"
            / f"strategy_family={STRATEGY_FAMILY}"
            / f"instrument={spec.symbol}"
            / f"year={spec.year}"
            / spec.quarter
            / "episodes.parquet"
        ),
    }
    for layer in LAYERS:
        paths[f"{layer}_parquet"] = (
            root
            / "parquet"
            / f"layer={layer}"
            / f"strategy_family={STRATEGY_FAMILY}"
            / f"instrument={spec.symbol}"
            / f"year={spec.year}"
            / spec.quarter
            / "advisory_rows.parquet"
        )
    return paths


def _run_output_paths(config: BackfillPilotConfig, *, run_id: str) -> dict[str, Path]:
    return {
        "manifest": config.output_root / "manifests" / run_id / "manifest.json",
        "validation_summary": config.output_root / "manifests" / run_id / "validation_summary.json",
    }


def _assert_run_outputs_do_not_exist(paths: Mapping[str, Path]) -> None:
    existing = [str(path) for path in paths.values() if path.exists()]
    if existing:
        raise SystemExit(json.dumps({"refusing_to_overwrite_existing_run_outputs": existing}, sort_keys=True))


def _partition_validation_summary(
    *,
    config: BackfillPilotConfig,
    run_id: str,
    spec: PartitionSpec,
    episodes: Sequence[Episode],
    advisory_rows_by_layer: Mapping[str, Sequence[Mapping[str, Any]]],
    raw_candidate_count: int,
    skipped_by_dedupe: int,
) -> dict[str, Any]:
    expected_rows_per_layer = len(episodes) * (config.window_bars + 1)
    failures: list[str] = []
    for layer in LAYERS:
        if len(advisory_rows_by_layer[layer]) != expected_rows_per_layer:
            failures.append(f"{layer}:ADVISORY_ROW_COUNT_MISMATCH")
    if any(row.get("runtime_trade_eligible") for rows in advisory_rows_by_layer.values() for row in rows):
        failures.append("RUNTIME_ELIGIBLE_ROW_PRESENT")
    if any(row.get("order_intent_created") for rows in advisory_rows_by_layer.values() for row in rows):
        failures.append("ORDER_INTENT_CREATED_ROW_PRESENT")
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "strategy_family": STRATEGY_FAMILY,
        "partition_id": spec.partition_id,
        "symbol": spec.symbol,
        "year": spec.year,
        "quarter": spec.quarter,
        "episode_count": len(episodes),
        "raw_candidate_count": raw_candidate_count,
        "skipped_by_dedupe": skipped_by_dedupe,
        "spillover_partitions_read": sorted(
            {partition for episode in episodes for partition in episode.spillover_partitions_read}
        ),
        "episodes_requiring_spillover": [
            episode.candidate_id for episode in episodes if episode.spillover_partitions_read
        ],
        "missing_spillover_failures": [],
        "window_bars": config.window_bars,
        "expected_rows_per_layer": expected_rows_per_layer,
        "advisory_row_count_by_layer": {layer: len(rows) for layer, rows in advisory_rows_by_layer.items()},
        "advisory_row_count": sum(len(rows) for rows in advisory_rows_by_layer.values()),
        "episode_centric": True,
        "global_bar_backfill": False,
        "failure_reasons": failures,
        "validation_passed": not failures,
        "runtime_activity": False,
        "strategy_behavior_changed": False,
        **SAFETY_FLAGS,
    }


def _partition_manifest(
    *,
    config: BackfillPilotConfig,
    run_id: str,
    spec: PartitionSpec,
    build_created_at: datetime,
    source_git_commit: str,
    raw_candidate_count: int,
    skipped_by_dedupe: int,
    episode_count: int,
    advisory_rows_by_layer: Mapping[str, Sequence[Mapping[str, Any]]],
    output_paths: Mapping[str, Path],
    source_inputs: Mapping[str, str],
    spillover_summary: Mapping[str, Any],
    validation_summary: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "build_name": BUILD_NAME,
        "run_id": run_id,
        "build_created_at": build_created_at.isoformat(),
        "source_git_commit": source_git_commit,
        "strategy_family": STRATEGY_FAMILY,
        "exit_profile": EXIT_PROFILE,
        "partition_id": spec.partition_id,
        "symbol": spec.symbol,
        "year": spec.year,
        "quarter": spec.quarter,
        "episode_window_bars": config.window_bars,
        "dedupe_bars": config.dedupe_bars,
        "episode_centric": True,
        "global_bar_backfill": False,
        "source_inputs": dict(source_inputs),
        "spillover_partitions_read": list(spillover_summary.get("spillover_partitions_read", [])),
        "episodes_requiring_spillover": list(spillover_summary.get("episodes_requiring_spillover", [])),
        "missing_spillover_failures": list(spillover_summary.get("missing_spillover_failures", [])),
        "raw_candidate_count": raw_candidate_count,
        "skipped_by_dedupe": skipped_by_dedupe,
        "episode_count": episode_count,
        "advisory_row_count": sum(len(rows) for rows in advisory_rows_by_layer.values()),
        "advisory_row_count_by_layer": {layer: len(rows) for layer, rows in advisory_rows_by_layer.items()},
        "output_paths": {key: str(value) for key, value in output_paths.items()},
        "validation_summary": dict(validation_summary),
        "validation_passed": bool(validation_summary.get("validation_passed")),
        "deleted_outputs": False,
        "source_mutated": False,
        "runtime_activity": False,
        "strategy_behavior_changed": False,
        **SAFETY_FLAGS,
    }


def _write_partition_outputs(result: Mapping[str, Any]) -> None:
    output_paths: Mapping[str, Path] = result["output_paths"]
    for layer, rows in result["advisory_rows_by_layer"].items():
        materialize_parquet_dataset(output_paths[f"{layer}_parquet"], rows)
    materialize_parquet_dataset(output_paths["episodes_parquet"], result["episode_rows"])
    write_storage_manifest(output_paths["partition_manifest"], result["partition_manifest"])
    write_storage_manifest(output_paths["partition_validation_summary"], result["partition_validation"])


def _validate_existing_partition(
    *,
    expected_manifest: Mapping[str, Any],
    output_paths: Mapping[str, Path],
) -> dict[str, Any]:
    missing = [str(path) for path in output_paths.values() if not path.exists()]
    if missing:
        raise SystemExit(json.dumps({"invalid_existing_partition": missing}, sort_keys=True))
    existing = json.loads(output_paths["partition_manifest"].read_text(encoding="utf-8"))
    failures: list[str] = []
    comparable_keys = (
        "schema_version",
        "strategy_family",
        "exit_profile",
        "partition_id",
        "symbol",
        "year",
        "quarter",
        "episode_window_bars",
        "dedupe_bars",
        "episode_centric",
        "global_bar_backfill",
        "raw_candidate_count",
        "skipped_by_dedupe",
        "episode_count",
        "advisory_row_count",
        "advisory_row_count_by_layer",
        "spillover_partitions_read",
        "episodes_requiring_spillover",
        "missing_spillover_failures",
        "source_inputs",
    )
    for key in comparable_keys:
        if existing.get(key) != expected_manifest.get(key):
            failures.append(f"EXISTING_PARTITION_{key.upper()}_MISMATCH")
    if existing.get("validation_passed") is not True:
        failures.append("EXISTING_PARTITION_VALIDATION_NOT_PASSED")
    if failures:
        raise SystemExit(
            json.dumps(
                {
                    "invalid_existing_partition": str(output_paths["partition_manifest"]),
                    "failure_reasons": failures,
                },
                sort_keys=True,
            )
        )
    return {"validated": True, "failure_reasons": []}


def _run_validation_summary(
    *,
    config: BackfillPilotConfig,
    run_id: str,
    partition_results: Sequence[Mapping[str, Any]],
    expected_counts: Mapping[str, int],
    quarter_counts: Mapping[str, int],
    runtime_seconds: float,
) -> dict[str, Any]:
    failures: list[str] = []
    for result in partition_results:
        failures.extend(f"{result['spec'].partition_id}:{reason}" for reason in result["partition_validation"]["failure_reasons"])
    failures.extend(
        _episode_reconciliation_failures(
            config=config,
            expected_counts=expected_counts,
            observed_quarter_counts=quarter_counts,
            total_episodes=sum(int(result["episode_count"]) for result in partition_results),
        )
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "strategy_family": STRATEGY_FAMILY,
        "symbols": list(config.symbols),
        "planned_partition_count": len(partition_results),
        "written_partition_count": sum(1 for result in partition_results if result["partition_action"] == "write"),
        "skipped_partition_count": sum(1 for result in partition_results if result["partition_action"] == "skip_existing"),
        "failed_partition_count": sum(1 for result in partition_results if result["partition_action"].startswith("failed")),
        "episode_count": sum(int(result["episode_count"]) for result in partition_results),
        "observed_quarter_episode_counts": dict(quarter_counts),
        "expected_episode_counts": dict(expected_counts),
        "spillover_partitions_read": sorted(
            {
                partition
                for result in partition_results
                for partition in result["spillover_summary"].get("spillover_partitions_read", [])
            }
        ),
        "episodes_requiring_spillover": [
            episode
            for result in partition_results
            for episode in result["spillover_summary"].get("episodes_requiring_spillover", [])
        ],
        "missing_spillover_failures": [
            failure
            for result in partition_results
            for failure in result["spillover_summary"].get("missing_spillover_failures", [])
        ],
        "window_bars": config.window_bars,
        "advisory_row_count_by_layer": _run_layer_counts(partition_results),
        "advisory_row_count": sum(_partition_advisory_row_count(result) for result in partition_results),
        "episode_centric": True,
        "global_bar_backfill": False,
        "runtime_seconds": runtime_seconds,
        "failure_reasons": failures,
        "validation_passed": not failures,
        "runtime_activity": False,
        "strategy_behavior_changed": False,
        **SAFETY_FLAGS,
    }


def _planned_partitions(config: BackfillPilotConfig) -> list[PartitionSpec]:
    if config.year is not None and config.quarter is not None:
        quarters = [(config.year, config.quarter)]
    else:
        assert config.start is not None and config.end is not None
        quarters = _quarters_between(config.start, config.end)
    return [PartitionSpec(symbol=symbol, year=year, quarter=quarter) for year, quarter in quarters for symbol in config.symbols]


def _quarters_between(start: datetime, end: datetime) -> list[tuple[int, str]]:
    current_year = start.year
    current_quarter = _quarter_number(start)
    end_key = (end.year, _quarter_number(end))
    quarters: list[tuple[int, str]] = []
    while (current_year, current_quarter) <= end_key:
        quarters.append((current_year, f"Q{current_quarter}"))
        current_quarter += 1
        if current_quarter == 5:
            current_quarter = 1
            current_year += 1
    return quarters


def _quarter_number(timestamp: datetime) -> int:
    return ((timestamp.month - 1) // 3) + 1


def _expected_episode_counts(config: BackfillPilotConfig) -> dict[str, int]:
    if config.expected_episode_counts_json is None:
        return {}
    payload = json.loads(config.expected_episode_counts_json.read_text(encoding="utf-8"))
    source = payload.get("by_year_quarter", payload)
    planned_quarters = {spec.shard_id for spec in _planned_partitions(config)}
    expected: dict[str, int] = {}
    for quarter_id in planned_quarters:
        value = source.get(quarter_id)
        if isinstance(value, Mapping):
            if "episode_count" not in value:
                raise SystemExit(f"expected episode counts JSON missing episode_count for {quarter_id}")
            expected[quarter_id] = int(value["episode_count"])
        elif value is not None:
            expected[quarter_id] = int(value)
        else:
            raise SystemExit(f"expected episode counts JSON missing {quarter_id}")
    return expected


def _quarter_episode_counts(partition_results: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for result in partition_results:
        spec: PartitionSpec = result["spec"]
        counts[spec.shard_id] = counts.get(spec.shard_id, 0) + int(result["episode_count"])
    return counts


def _episode_reconciliation_failures(
    *,
    config: BackfillPilotConfig,
    expected_counts: Mapping[str, int],
    observed_quarter_counts: Mapping[str, int],
    total_episodes: int,
) -> list[str]:
    failures: list[str] = []
    if config.expected_episodes is not None and total_episodes != config.expected_episodes:
        failures.append("EXPECTED_TOTAL_EPISODE_COUNT_MISMATCH")
    for quarter_id, expected in expected_counts.items():
        if observed_quarter_counts.get(quarter_id) != expected:
            failures.append(f"EXPECTED_QUARTER_EPISODE_COUNT_MISMATCH:{quarter_id}")
    return failures


def _partition_summary(result: Mapping[str, Any]) -> dict[str, Any]:
    spec: PartitionSpec = result["spec"]
    return {
        "partition_id": spec.partition_id,
        "symbol": spec.symbol,
        "year": spec.year,
        "quarter": spec.quarter,
        "partition_action": result["partition_action"],
        "episode_count": result["episode_count"],
        "advisory_row_count": _partition_advisory_row_count(result),
        "spillover_partitions_read": list(result["spillover_summary"].get("spillover_partitions_read", [])),
        "episodes_requiring_spillover": list(result["spillover_summary"].get("episodes_requiring_spillover", [])),
        "partition_manifest_path": str(result["output_paths"]["partition_manifest"]),
    }


def _counts_by_partition(partition_results: Sequence[Mapping[str, Any]], key: str) -> dict[str, int]:
    return {result["spec"].partition_id: int(result[key]) for result in partition_results}


def _partition_advisory_row_count(result: Mapping[str, Any]) -> int:
    return sum(len(rows) for rows in result["advisory_rows_by_layer"].values())


def _run_layer_counts(partition_results: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    totals = {layer: 0 for layer in LAYERS}
    for result in partition_results:
        for layer, rows in result["advisory_rows_by_layer"].items():
            totals[layer] += len(rows)
    return totals


def _summary(result: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": result["schema_version"],
        "run_id": result["run_id"],
        "mode": result["mode"],
        "symbols": result["symbols"],
        "planned_partition_count": len(result["planned_partitions"]),
        "written_partition_count": len(result["written_partitions"]),
        "skipped_partition_count": len(result["skipped_partitions"]),
        "failed_partition_count": len(result["failed_partitions"]),
        "episode_count": result["episode_count"],
        "advisory_row_count": result["advisory_row_count"],
        "advisory_row_count_by_layer": result["advisory_row_count_by_layer"],
        "runtime_seconds": result["runtime_seconds"],
        "manifest_path": result["output_paths"]["manifest"],
        "validation_summary_path": result["validation_summary_path"],
        "validation_passed": result["validation_summary"]["validation_passed"],
        "failure_reasons": result["validation_summary"]["failure_reasons"],
        "wrote_outputs": result["wrote_outputs"],
        "runtime_activity": False,
        "strategy_behavior_changed": False,
        "deleted_outputs": False,
    }


def _layer_contract(layer: str) -> str:
    return {
        "lifecycle_awareness": "track_b_lifecycle_awareness_state_v1",
        "participation_pressure": "participation_pressure_context_v1",
        "regime_session": "track_b_regime_session_context_v1",
        "sizing_position_management": "track_b_sizing_position_management_state_v1",
    }[layer]


def _source_window_hash(rows: Sequence[Mapping[str, Any]]) -> str:
    payload = "|".join(
        f"{_coerce_ts(row['bar_ts']).isoformat()}:{row['open']}:{row['high']}:{row['low']}:{row['close']}:{row['volume']}"
        for row in rows
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _stable_id(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:24]


def _source_git_commit() -> str:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return "UNKNOWN"
    return completed.stdout.strip()


def _parse_symbols(raw: str) -> list[str]:
    symbols = [item.strip().upper() for item in raw.split(",") if item.strip()]
    if not symbols:
        raise SystemExit("--symbols must contain at least one symbol")
    return symbols


def _parse_optional_timestamp(raw: str | None) -> datetime | None:
    if raw is None:
        return None
    normalized = raw.strip()
    if len(normalized) == 10:
        normalized = f"{normalized}T00:00:00+00:00"
    return _coerce_ts(normalized)


def _coerce_ts(value: Any) -> datetime:
    if isinstance(value, datetime):
        timestamp = value
    else:
        timestamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=UTC)
    return timestamp.astimezone(UTC)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
