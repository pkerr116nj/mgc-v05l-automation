"""Generated artifact retention and rotation utility.

This module only operates on configured generated artifact roots. It refuses
source/config/test/script paths and protects current Track B broker, registry,
lifecycle, reconciliation, and latest authority artifacts.
"""

from __future__ import annotations

import argparse
import fnmatch
import gzip
import json
import os
import shutil
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_POLICY_PATH = Path("config/generated_artifact_retention.json")
DEFAULT_MAINTENANCE_LOG = (
    Path("outputs")
    / "track_b_execution_core"
    / "artifact_retention"
    / "generated_artifact_retention_events.jsonl"
)

CHECK_OK = "GENERATED_ARTIFACT_RETENTION_CHECK_OK"
CHECK_WARN = "GENERATED_ARTIFACT_RETENTION_CHECK_WARN"
MAINTENANCE_DRY_RUN = "GENERATED_ARTIFACT_RETENTION_MAINTENANCE_DRY_RUN"
MAINTENANCE_APPLIED = "GENERATED_ARTIFACT_RETENTION_MAINTENANCE_APPLIED"
ROTATED = "ROTATED"
SKIPPED_PROTECTED = "SKIPPED_PROTECTED"
SKIPPED_UNDER_THRESHOLD = "SKIPPED_UNDER_THRESHOLD"
SKIPPED_UNSAFE_PATH = "SKIPPED_UNSAFE_PATH"


@dataclass(frozen=True)
class StreamPolicy:
    stream_id: str
    patterns: tuple[str, ...]
    max_file_size_mb: int
    keep_latest_tail_mb: int
    max_archives_per_stream: int
    compression_enabled: bool = True
    recreate_live_file: bool = True
    description: str = ""

    @property
    def max_file_size_bytes(self) -> int:
        return int(self.max_file_size_mb) * 1024 * 1024

    @property
    def keep_latest_tail_bytes(self) -> int:
        return max(0, int(self.keep_latest_tail_mb)) * 1024 * 1024


@dataclass(frozen=True)
class RetentionPolicy:
    max_file_size_mb: int
    keep_latest_tail_mb: int
    max_archives_per_stream: int
    compression_enabled: bool
    generated_roots: tuple[str, ...]
    forbidden_roots: tuple[str, ...]
    protected_globs: tuple[str, ...]
    streams: tuple[StreamPolicy, ...]

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "RetentionPolicy":
        default_max_mb = int(payload.get("max_file_size_mb") or 512)
        default_tail_mb = int(payload.get("keep_latest_tail_mb") or 256)
        default_archives = int(payload.get("max_archives_per_stream") or 5)
        default_compression = bool(payload.get("compression_enabled", True))
        streams = []
        for item in _list(payload.get("streams")):
            streams.append(
                StreamPolicy(
                    stream_id=str(item["stream_id"]),
                    patterns=tuple(str(pattern) for pattern in _list(item.get("patterns"))),
                    max_file_size_mb=int(item.get("max_file_size_mb") or default_max_mb),
                    keep_latest_tail_mb=int(item.get("keep_latest_tail_mb") or 0),
                    max_archives_per_stream=int(item.get("max_archives_per_stream") or default_archives),
                    compression_enabled=bool(item.get("compression_enabled", default_compression)),
                    recreate_live_file=bool(item.get("recreate_live_file", True)),
                    description=str(item.get("description") or ""),
                )
            )
        return cls(
            max_file_size_mb=default_max_mb,
            keep_latest_tail_mb=default_tail_mb,
            max_archives_per_stream=default_archives,
            compression_enabled=default_compression,
            generated_roots=tuple(str(item).strip("/") for item in _list(payload.get("generated_roots"))),
            forbidden_roots=tuple(str(item).strip("/") for item in _list(payload.get("forbidden_roots"))),
            protected_globs=tuple(str(item) for item in _list(payload.get("protected_globs"))),
            streams=tuple(streams),
        )


@dataclass(frozen=True)
class RetentionConfig:
    repo_root: Path = REPO_ROOT
    policy_path: Path = DEFAULT_POLICY_PATH
    maintenance_log_path: Path = DEFAULT_MAINTENANCE_LOG
    top_limit: int = 20

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def load_policy(*, config: RetentionConfig) -> RetentionPolicy:
    path = config.resolve(config.policy_path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    return RetentionPolicy.from_mapping(payload)


def check_generated_artifacts(*, config: RetentionConfig, policy: RetentionPolicy | None = None) -> dict[str, Any]:
    actual_policy = policy or load_policy(config=config)
    candidates = list(_iter_stream_candidates(config=config, policy=actual_policy))
    violations = [
        item
        for item in candidates
        if item["exists"]
        and not item["protected"]
        and item["safe_to_rotate"]
        and int(item["size_bytes"]) > int(item["max_file_size_bytes"])
    ]
    protected_over_threshold = [
        item
        for item in candidates
        if item["exists"] and item["protected"] and int(item["size_bytes"]) > int(item["max_file_size_bytes"])
    ]
    large_files = _top_large_generated_files(config=config, policy=actual_policy, limit=config.top_limit)
    classification = CHECK_WARN if violations else CHECK_OK
    return {
        "schema_version": "generated_artifact_retention_check_v1",
        "generated_at": _now().isoformat(),
        "classification": classification,
        "repo_root": str(config.repo_root),
        "read_only": True,
        "broker_mutation": False,
        "runtime_restart": False,
        "source_mutation": False,
        "candidate_count": len(candidates),
        "violation_count": len(violations),
        "protected_over_threshold_count": len(protected_over_threshold),
        "violations": violations[: config.top_limit],
        "protected_over_threshold": protected_over_threshold[: config.top_limit],
        "top_large_files": large_files,
    }


def maintain_generated_artifacts(
    *,
    config: RetentionConfig,
    apply: bool,
    policy: RetentionPolicy | None = None,
) -> dict[str, Any]:
    actual_policy = policy or load_policy(config=config)
    actions: list[dict[str, Any]] = []
    for candidate in _iter_stream_candidates(config=config, policy=actual_policy):
        action = _maintenance_action(config=config, policy=actual_policy, candidate=candidate, apply=apply)
        actions.append(action)
    rotated = [item for item in actions if item.get("status") == ROTATED]
    payload = {
        "schema_version": "generated_artifact_retention_maintenance_v1",
        "generated_at": _now().isoformat(),
        "classification": MAINTENANCE_APPLIED if apply else MAINTENANCE_DRY_RUN,
        "repo_root": str(config.repo_root),
        "apply": apply,
        "dry_run": not apply,
        "broker_mutation": False,
        "runtime_restart": False,
        "source_mutation": False,
        "rotated_count": len(rotated),
        "action_count": len(actions),
        "actions": actions,
    }
    if apply and rotated:
        _append_jsonl(config.resolve(config.maintenance_log_path), payload)
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Check or rotate generated runtime artifacts.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY_PATH)
    parser.add_argument("--top-limit", type=int, default=20)
    parser.add_argument("--json", action="store_true", help="Print full JSON output.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    check = subparsers.add_parser("check", help="Report large generated artifacts and hot-path threshold breaches.")
    check.add_argument("--warn-only", action="store_true", help="Always exit 0 even when thresholds are exceeded.")
    check.add_argument("--json", action="store_true", dest="json_local", help="Print full JSON output.")

    maintain = subparsers.add_parser("maintain", help="Rotate oversized generated artifacts.")
    mode = maintain.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="Report what would rotate without mutating files.")
    mode.add_argument("--apply", action="store_true", help="Rotate oversized artifacts and write a maintenance log.")
    maintain.add_argument("--json", action="store_true", dest="json_local", help="Print full JSON output.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = RetentionConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        policy_path=Path(args.policy),
        top_limit=int(args.top_limit),
    )
    if args.command == "check":
        payload = check_generated_artifacts(config=config)
        _print_payload(payload=payload, full=bool(args.json or args.json_local))
        if payload["classification"] == CHECK_WARN and not bool(args.warn_only):
            return 2
        return 0
    if args.command == "maintain":
        payload = maintain_generated_artifacts(config=config, apply=bool(args.apply))
        _print_payload(payload=payload, full=bool(args.json or args.json_local))
        return 0
    raise AssertionError(f"Unhandled command: {args.command}")


def _maintenance_action(
    *,
    config: RetentionConfig,
    policy: RetentionPolicy,
    candidate: Mapping[str, Any],
    apply: bool,
) -> dict[str, Any]:
    path = Path(str(candidate["absolute_path"]))
    base = {
        "stream_id": candidate["stream_id"],
        "relative_path": candidate["relative_path"],
        "size_bytes": candidate["size_bytes"],
        "max_file_size_bytes": candidate["max_file_size_bytes"],
        "apply": apply,
    }
    if not candidate["exists"] or int(candidate["size_bytes"]) <= int(candidate["max_file_size_bytes"]):
        return {**base, "status": SKIPPED_UNDER_THRESHOLD}
    if candidate["protected"]:
        return {**base, "status": SKIPPED_PROTECTED, "reason": candidate.get("protection_reason")}
    if not candidate["safe_to_rotate"]:
        return {**base, "status": SKIPPED_UNSAFE_PATH, "reason": candidate.get("unsafe_reason")}
    stream = _stream_for_id(policy=policy, stream_id=str(candidate["stream_id"]))
    archive_path = _archive_path(config=config, stream=stream, source=path)
    tail_path = _tail_path(path)
    planned = {
        **base,
        "status": "WOULD_ROTATE",
        "tail_path": str(tail_path),
        "archive_path": str(archive_path),
        "keep_latest_tail_bytes": stream.keep_latest_tail_bytes,
        "compression_enabled": stream.compression_enabled,
        "recreate_live_file": stream.recreate_live_file,
    }
    if not apply:
        return planned

    if stream.keep_latest_tail_bytes > 0:
        _write_tail(source=path, destination=tail_path, max_bytes=stream.keep_latest_tail_bytes)
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    if stream.compression_enabled:
        tmp_path = archive_path.with_suffix(archive_path.suffix + ".tmp")
        try:
            with path.open("rb") as src, gzip.open(tmp_path, "wb", compresslevel=6) as dst:
                shutil.copyfileobj(src, dst, length=1024 * 1024)
            _verify_gzip(tmp_path)
            tmp_path.replace(archive_path)
        finally:
            if tmp_path.exists():
                tmp_path.unlink()
    else:
        shutil.move(str(path), str(archive_path))
    if stream.compression_enabled:
        path.unlink()
    if stream.recreate_live_file:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
    _prune_archives(archive_path.parent, max_archives=stream.max_archives_per_stream)
    return {
        **planned,
        "status": ROTATED,
        "tail_exists": tail_path.exists() if stream.keep_latest_tail_bytes > 0 else False,
        "archive_exists": archive_path.exists(),
        "live_exists": path.exists(),
        "live_size_bytes": path.stat().st_size if path.exists() else None,
    }


def _iter_stream_candidates(*, config: RetentionConfig, policy: RetentionPolicy) -> Iterable[dict[str, Any]]:
    seen: set[Path] = set()
    for stream in policy.streams:
        for pattern in stream.patterns:
            for path in config.repo_root.glob(pattern):
                resolved = path.resolve()
                if resolved in seen or not resolved.is_file():
                    continue
                seen.add(resolved)
                relative = _relative_posix(resolved, config.repo_root)
                protected, reason = _protected(relative=relative, policy=policy)
                safe, unsafe_reason = _safe_to_rotate(relative=relative, policy=policy)
                size = resolved.stat().st_size
                yield {
                    "stream_id": stream.stream_id,
                    "relative_path": relative,
                    "absolute_path": str(resolved),
                    "exists": True,
                    "size_bytes": size,
                    "size_mb": round(size / 1024 / 1024, 3),
                    "max_file_size_bytes": stream.max_file_size_bytes,
                    "max_file_size_mb": stream.max_file_size_mb,
                    "protected": protected,
                    "protection_reason": reason,
                    "safe_to_rotate": safe,
                    "unsafe_reason": unsafe_reason,
                }


def _top_large_generated_files(
    *,
    config: RetentionConfig,
    policy: RetentionPolicy,
    limit: int,
) -> list[dict[str, Any]]:
    suffixes = {".jsonl", ".log", ".db", ".sqlite", ".csv", ".json", ".md"}
    files: list[dict[str, Any]] = []
    for root_name in policy.generated_roots:
        root = config.resolve(Path(root_name))
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if not path.is_file() or path.suffix not in suffixes:
                continue
            relative = _relative_posix(path.resolve(), config.repo_root)
            size = path.stat().st_size
            protected, reason = _protected(relative=relative, policy=policy)
            files.append(
                {
                    "relative_path": relative,
                    "size_bytes": size,
                    "size_mb": round(size / 1024 / 1024, 3),
                    "protected": protected,
                    "protection_reason": reason,
                }
            )
    return sorted(files, key=lambda item: int(item["size_bytes"]), reverse=True)[:limit]


def _safe_to_rotate(*, relative: str, policy: RetentionPolicy) -> tuple[bool, str | None]:
    first = relative.split("/", 1)[0]
    if first in set(policy.forbidden_roots):
        return False, f"forbidden root: {first}"
    if not any(relative == root or relative.startswith(f"{root}/") for root in policy.generated_roots):
        return False, "not under configured generated roots"
    return True, None


def _protected(*, relative: str, policy: RetentionPolicy) -> tuple[bool, str | None]:
    for pattern in policy.protected_globs:
        if fnmatch.fnmatchcase(relative, pattern):
            return True, f"matched protected policy glob: {pattern}"
    return False, None


def _write_tail(*, source: Path, destination: Path, max_bytes: int) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with source.open("rb") as src:
        src.seek(0, os.SEEK_END)
        size = src.tell()
        start = max(0, size - max_bytes)
        src.seek(start)
        data = src.read()
    if start > 0:
        newline = data.find(b"\n")
        if newline >= 0 and newline + 1 < len(data):
            data = data[newline + 1 :]
    destination.write_bytes(data)


def _archive_path(*, config: RetentionConfig, stream: StreamPolicy, source: Path) -> Path:
    stamp = _now().strftime("%Y%m%dT%H%M%SZ")
    suffix = "".join(source.suffixes) or source.suffix
    stem = source.name[: -len(suffix)] if suffix and source.name.endswith(suffix) else source.stem
    archive_name = f"{stem}_{stamp}{suffix}"
    if stream.compression_enabled:
        archive_name += ".gz"
    archive_root = config.repo_root / "outputs" / "archive" / stream.stream_id
    candidate = archive_root / archive_name
    index = 1
    while candidate.exists():
        candidate = archive_root / archive_name.replace(suffix, f"_{index}{suffix}", 1)
        index += 1
    return candidate


def _tail_path(path: Path) -> Path:
    suffixes = "".join(path.suffixes)
    if suffixes:
        return path.with_name(f"{path.name[: -len(suffixes)]}.recent_tail{suffixes}")
    return path.with_name(f"{path.name}.recent_tail")


def _verify_gzip(path: Path) -> None:
    with gzip.open(path, "rb") as handle:
        while handle.read(1024 * 1024):
            pass


def _prune_archives(archive_dir: Path, *, max_archives: int) -> None:
    if max_archives <= 0 or not archive_dir.exists():
        return
    archives = sorted((path for path in archive_dir.iterdir() if path.is_file()), key=lambda path: path.stat().st_mtime)
    for old in archives[: max(0, len(archives) - max_archives)]:
        old.unlink()


def _stream_for_id(*, policy: RetentionPolicy, stream_id: str) -> StreamPolicy:
    for stream in policy.streams:
        if stream.stream_id == stream_id:
            return stream
    raise KeyError(stream_id)


def _append_jsonl(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(dict(payload), sort_keys=True) + "\n")


def _print_payload(*, payload: Mapping[str, Any], full: bool) -> None:
    if full:
        print(json.dumps(dict(payload), indent=2, sort_keys=True))
        return
    summary_keys = (
        "classification",
        "candidate_count",
        "violation_count",
        "protected_over_threshold_count",
        "rotated_count",
        "action_count",
    )
    summary = {key: payload[key] for key in summary_keys if key in payload}
    print(json.dumps(summary, indent=2, sort_keys=True))


def _relative_posix(path: Path, repo_root: Path) -> str:
    try:
        return path.relative_to(repo_root).as_posix()
    except ValueError:
        return path.as_posix()


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _now() -> datetime:
    return datetime.now(UTC)


if __name__ == "__main__":
    sys.exit(main())
