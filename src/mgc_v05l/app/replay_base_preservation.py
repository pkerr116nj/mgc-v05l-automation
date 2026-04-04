"""Non-destructive preservation helpers for the foundational replay DB."""

from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Iterator

REPO_ROOT = Path.cwd()
DEFAULT_PRIMARY_DB_PATH = REPO_ROOT / "mgc_v05l.replay.sqlite3"
DEFAULT_ARCHIVE_DB_PATH = REPO_ROOT / "mgc_v05l.replay.archive.sqlite3"
DEFAULT_REPORT_DIR = REPO_ROOT / "outputs" / "reports" / "replay_base_preservation"

CURRENT_BARS_INSERT_COLUMNS = (
    "bar_id",
    "ticker",
    "cusip",
    "asset_class",
    "data_source",
    "timestamp",
    "symbol",
    "timeframe",
    "start_ts",
    "end_ts",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "is_final",
    "session_asia",
    "session_london",
    "session_us",
    "session_allowed",
    "created_at",
)


@dataclass(frozen=True)
class ReplayCoverageSummary:
    path: str
    exists: bool
    bars_table_present: bool
    schema_kind: str | None
    rows_1m: int
    earliest_1m: str | None
    latest_1m: str | None
    rows_in_gap_window: int
    earliest_in_gap_window: str | None
    latest_in_gap_window: str | None


def preserve_replay_base(
    *,
    primary_db_path: Path = DEFAULT_PRIMARY_DB_PATH,
    archive_db_path: Path = DEFAULT_ARCHIVE_DB_PATH,
    check_paths: Iterable[Path] = (),
    report_dir: Path | None = None,
) -> dict[str, Any]:
    primary_db_path = primary_db_path.resolve()
    archive_db_path = archive_db_path.resolve()
    checked_paths = _unique_paths([primary_db_path, archive_db_path, *check_paths])

    before = {str(path): asdict(_summarize_db(path)) for path in checked_paths}
    actions: list[dict[str, Any]] = []

    if primary_db_path.exists() and not archive_db_path.exists():
        archive_db_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(primary_db_path, archive_db_path)
        actions.append(
            {
                "action": "bootstrap_archive_from_primary",
                "source": str(primary_db_path),
                "destination": str(archive_db_path),
            }
        )
    elif archive_db_path.exists() and not primary_db_path.exists():
        primary_db_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(archive_db_path, primary_db_path)
        actions.append(
            {
                "action": "restore_primary_from_archive",
                "source": str(archive_db_path),
                "destination": str(primary_db_path),
            }
        )

    if primary_db_path.exists() and archive_db_path.exists():
        merged_archive = _merge_source_into_current_schema(destination=archive_db_path, source=primary_db_path)
        merged_primary = _merge_source_into_current_schema(destination=primary_db_path, source=archive_db_path)
        if merged_archive:
            actions.append(
                {
                    "action": "merge_primary_into_archive",
                    "source": str(primary_db_path),
                    "destination": str(archive_db_path),
                    "rows_inserted": merged_archive,
                }
            )
        if merged_primary:
            actions.append(
                {
                    "action": "merge_archive_into_primary",
                    "source": str(archive_db_path),
                    "destination": str(primary_db_path),
                    "rows_inserted": merged_primary,
                }
            )

    after = {str(path): asdict(_summarize_db(path)) for path in checked_paths}
    primary_before = before.get(str(primary_db_path), {})
    archive_before = before.get(str(archive_db_path), {})
    primary_after = after.get(str(primary_db_path), {})
    archive_after = after.get(str(archive_db_path), {})
    regression_detected = _is_shallower(primary_before, archive_before)

    warnings: list[str] = []
    if regression_detected:
        warnings.append(
            "Primary replay DB was shallower than the preserved archive before sync; archive coverage was used to heal the regression."
        )

    payload: dict[str, Any] = {
        "generated_at": datetime.now(UTC).isoformat(),
        "primary_db_path": str(primary_db_path),
        "archive_db_path": str(archive_db_path),
        "checked_storage_locations": [str(path) for path in checked_paths],
        "before": before,
        "after": after,
        "actions": actions,
        "regression_detected_before_sync": regression_detected,
        "status": "warning" if warnings else "ok",
        "warnings": warnings,
        "gap_window": {
            "start": "2026-02-03",
            "end_exclusive": "2026-02-15",
            "exists_in_primary_after_sync": int(primary_after.get("rows_in_gap_window") or 0) > 0,
            "exists_in_archive_after_sync": int(archive_after.get("rows_in_gap_window") or 0) > 0,
        },
        "preservation_rule": (
            "The archive DB is never allowed to become shallower than the primary replay DB. "
            "Before and after backfill/regeneration runs, compatible bars are merged both directions "
            "so a newly created or narrower primary cannot silently replace deeper stored history."
        ),
    }
    if report_dir is not None:
        report_dir.mkdir(parents=True, exist_ok=True)
        run_stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        report_path = report_dir / f"replay_base_preservation_{run_stamp}.json"
        report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        latest_path = report_dir / "latest.json"
        latest_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        payload["report_path"] = str(report_path)
        payload["latest_report_path"] = str(latest_path)
    return payload


def _unique_paths(paths: Iterable[Path]) -> list[Path]:
    unique: list[Path] = []
    seen: set[str] = set()
    for path in paths:
        resolved = path.resolve()
        key = str(resolved)
        if key in seen:
            continue
        seen.add(key)
        unique.append(resolved)
    return unique


def _summarize_db(path: Path) -> ReplayCoverageSummary:
    if not path.exists():
        return ReplayCoverageSummary(
            path=str(path),
            exists=False,
            bars_table_present=False,
            schema_kind=None,
            rows_1m=0,
            earliest_1m=None,
            latest_1m=None,
            rows_in_gap_window=0,
            earliest_in_gap_window=None,
            latest_in_gap_window=None,
        )

    connection = sqlite3.connect(path)
    try:
        if not _table_exists(connection, "bars"):
            return ReplayCoverageSummary(
                path=str(path),
                exists=True,
                bars_table_present=False,
                schema_kind=None,
                rows_1m=0,
                earliest_1m=None,
                latest_1m=None,
                rows_in_gap_window=0,
                earliest_in_gap_window=None,
                latest_in_gap_window=None,
            )
        schema_kind = _bars_schema_kind(connection)
        if schema_kind is None:
            return ReplayCoverageSummary(
                path=str(path),
                exists=True,
                bars_table_present=True,
                schema_kind=None,
                rows_1m=0,
                earliest_1m=None,
                latest_1m=None,
                rows_in_gap_window=0,
                earliest_in_gap_window=None,
                latest_in_gap_window=None,
            )
        row = connection.execute(
            """
            select count(*), min(end_ts), max(end_ts)
            from bars
            where timeframe = '1m'
            """
        ).fetchone()
        gap_row = connection.execute(
            """
            select count(*), min(end_ts), max(end_ts)
            from bars
            where timeframe = '1m'
              and end_ts >= '2026-02-03'
              and end_ts < '2026-02-15'
            """
        ).fetchone()
        return ReplayCoverageSummary(
            path=str(path),
            exists=True,
            bars_table_present=True,
            schema_kind=schema_kind,
            rows_1m=int(row[0] or 0),
            earliest_1m=str(row[1]) if row[1] else None,
            latest_1m=str(row[2]) if row[2] else None,
            rows_in_gap_window=int(gap_row[0] or 0),
            earliest_in_gap_window=str(gap_row[1]) if gap_row[1] else None,
            latest_in_gap_window=str(gap_row[2]) if gap_row[2] else None,
        )
    finally:
        connection.close()


def _is_shallower(primary_before: dict[str, Any], archive_before: dict[str, Any]) -> bool:
    primary_earliest = str(primary_before.get("earliest_1m") or "")
    archive_earliest = str(archive_before.get("earliest_1m") or "")
    if not primary_earliest or not archive_earliest:
        return False
    return primary_earliest > archive_earliest


def _table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    row = connection.execute(
        "select 1 from sqlite_master where type = 'table' and name = ? limit 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _bars_schema_kind(connection: sqlite3.Connection) -> str | None:
    columns = {str(row[1]) for row in connection.execute("pragma table_info(bars)").fetchall()}
    current_required = {"bar_id", "ticker", "data_source", "timestamp", "symbol", "timeframe", "start_ts", "end_ts", "created_at"}
    legacy_required = {"bar_id", "symbol", "timeframe", "start_ts", "end_ts"}
    if current_required.issubset(columns):
        return "current"
    if legacy_required.issubset(columns):
        return "legacy"
    return None


def _merge_source_into_current_schema(*, destination: Path, source: Path) -> int:
    if not destination.exists() or not source.exists() or destination == source:
        return 0

    source_connection = sqlite3.connect(source)
    destination_connection = sqlite3.connect(destination)
    inserted = 0
    try:
        if not _table_exists(destination_connection, "bars") or not _table_exists(source_connection, "bars"):
            return 0
        destination_kind = _bars_schema_kind(destination_connection)
        source_kind = _bars_schema_kind(source_connection)
        if destination_kind != "current" or source_kind is None:
            return 0
        statement = (
            f"insert or ignore into bars ({', '.join(CURRENT_BARS_INSERT_COLUMNS)}) "
            f"values ({', '.join(['?'] * len(CURRENT_BARS_INSERT_COLUMNS))})"
        )
        before_changes = destination_connection.total_changes
        destination_connection.execute("pragma journal_mode = wal")
        for batch in _iter_normalized_bar_batches(source_connection, source_kind):
            if batch:
                destination_connection.executemany(statement, batch)
        destination_connection.commit()
        inserted = destination_connection.total_changes - before_changes
        return inserted
    finally:
        source_connection.close()
        destination_connection.close()


def _iter_normalized_bar_batches(
    source_connection: sqlite3.Connection,
    source_kind: str,
    *,
    batch_size: int = 5000,
) -> Iterator[list[tuple[Any, ...]]]:
    now_iso = datetime.now(UTC).isoformat()
    if source_kind == "current":
        cursor = source_connection.execute(
            """
            select bar_id, ticker, cusip, asset_class, data_source, timestamp, symbol, timeframe,
                   start_ts, end_ts, open, high, low, close, volume, is_final,
                   session_asia, session_london, session_us, session_allowed, created_at
            from bars
            order by end_ts asc
            """
        )
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            yield [tuple(row) for row in rows]
        return

    cursor = source_connection.execute(
        """
        select bar_id, symbol, timeframe, start_ts, end_ts, open, high, low, close, volume,
               is_final, session_asia, session_london, session_us, session_allowed
        from bars
        order by end_ts asc
        """
    )
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        batch: list[tuple[Any, ...]] = []
        for row in rows:
            symbol = str(row[1] or "")
            end_ts = str(row[4] or "")
            batch.append(
                (
                    row[0],
                    symbol,
                    None,
                    None,
                    "legacy_replay",
                    end_ts,
                    symbol,
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[7],
                    row[8],
                    row[9],
                    row[10],
                    row[11],
                    row[12],
                    row[13],
                    row[14],
                    now_iso,
                )
            )
        yield batch


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Preserve and audit the foundational replay 1m base.")
    parser.add_argument("--primary-db-path", default=str(DEFAULT_PRIMARY_DB_PATH))
    parser.add_argument("--archive-db-path", default=str(DEFAULT_ARCHIVE_DB_PATH))
    parser.add_argument("--check-path", action="append", default=[], help="Additional DB paths to audit.")
    parser.add_argument("--write-report", action="store_true", help="Write a timestamped JSON audit report.")
    parser.add_argument(
        "--fail-on-regression",
        action="store_true",
        help="Exit non-zero if the primary replay DB is found to be shallower than the preserved archive before sync.",
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    payload = preserve_replay_base(
        primary_db_path=Path(args.primary_db_path),
        archive_db_path=Path(args.archive_db_path),
        check_paths=[Path(path) for path in args.check_path],
        report_dir=DEFAULT_REPORT_DIR if args.write_report else None,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    if args.fail_on_regression and payload.get("regression_detected_before_sync"):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
