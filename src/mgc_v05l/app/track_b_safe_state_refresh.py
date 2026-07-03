"""CLI for read-only Track B Safe-State envelope refresh."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_runtime_safe_state_envelope import (
    DEFAULT_RUNTIME_SAFE_STATE_ENVELOPE_ARTIFACT,
    REPO_ROOT,
    SAFE_STATE_NORMAL,
    TrackBRuntimeSafeStateEnvelopeConfig,
    refresh_track_b_runtime_safe_state_envelope,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Refresh the read-only Track B Safe-State envelope.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_RUNTIME_SAFE_STATE_ENVELOPE_ARTIFACT)
    parser.add_argument("--json", action="store_true", help="Print the full refreshed envelope.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBRuntimeSafeStateEnvelopeConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        output_path=Path(args.output_path),
    )
    result = refresh_track_b_runtime_safe_state_envelope(config=config)
    print(json.dumps(result.payload if args.json else result.summary, indent=2, sort_keys=True))
    return 0 if result.payload.get("safe_state_classification") == SAFE_STATE_NORMAL else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
