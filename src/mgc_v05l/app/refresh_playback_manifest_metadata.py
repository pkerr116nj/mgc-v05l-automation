"""Refresh latest historical-playback manifest with lightweight study metadata."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from .strategy_study import build_strategy_study_catalog_entry, build_strategy_study_preview

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PLAYBACK_DIR = REPO_ROOT / "outputs" / "historical_playback"


def refresh_playback_manifest_metadata(*, manifest_path: str | Path | None = None) -> Path:
    resolved_manifest_path = (
        Path(manifest_path).resolve()
        if manifest_path is not None
        else _latest_manifest_path(DEFAULT_PLAYBACK_DIR)
    )
    if resolved_manifest_path is None or not resolved_manifest_path.exists():
        raise FileNotFoundError("No historical-playback manifest found to refresh.")
    payload = json.loads(resolved_manifest_path.read_text(encoding="utf-8"))
    run_stamp = str(payload.get("run_stamp") or resolved_manifest_path.stem)
    run_timestamp = datetime.fromtimestamp(resolved_manifest_path.stat().st_mtime, tz=UTC).isoformat()
    changed = False
    for entry in list(payload.get("symbols") or []):
        study_json_path = entry.get("strategy_study_json_path")
        if not study_json_path:
            continue
        study_payload = json.loads(Path(study_json_path).read_text(encoding="utf-8"))
        preview = build_strategy_study_preview(study_payload)
        catalog_entry = build_strategy_study_catalog_entry(
            payload=study_payload,
            run_stamp=run_stamp,
            run_timestamp=run_timestamp,
            manifest_path=str(resolved_manifest_path),
            summary_path=entry.get("summary_path"),
            strategy_study_json_path=entry.get("strategy_study_json_path"),
            strategy_study_markdown_path=entry.get("strategy_study_markdown_path"),
            label=entry.get("label"),
        )
        if entry.get("study_preview") != preview:
            entry["study_preview"] = preview
            changed = True
        if entry.get("catalog_entry") != catalog_entry:
            entry["catalog_entry"] = catalog_entry
            changed = True
    if changed:
        resolved_manifest_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return resolved_manifest_path


def _latest_manifest_path(playback_dir: Path) -> Path | None:
    manifests = sorted(playback_dir.glob("historical_playback_*.manifest.json"), key=lambda path: path.stat().st_mtime)
    return manifests[-1] if manifests else None


def main() -> int:
    parser = argparse.ArgumentParser(prog="refresh-playback-manifest-metadata")
    parser.add_argument("--manifest-path", default=None)
    args = parser.parse_args()
    manifest_path = refresh_playback_manifest_metadata(manifest_path=args.manifest_path)
    print(json.dumps({"manifest_path": str(manifest_path)}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
