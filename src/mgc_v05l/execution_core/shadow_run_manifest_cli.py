"""CLI wrapper for Track B no-submit shadow run manifest validation."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .shadow_run_manifest import DEFAULT_SHADOW_RUN_MANIFEST_OUTPUT_ROOT, ShadowRunManifestVerdict, validate_shadow_run_manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate a Track B no-submit shadow run manifest.")
    parser.add_argument("--manifest-json", required=True, type=Path)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_SHADOW_RUN_MANIFEST_OUTPUT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = json.loads(args.manifest_json.read_text(encoding="utf-8"))
    result = validate_shadow_run_manifest(payload=payload, output_root=args.output_root)
    print(
        json.dumps(
            {
                "manifest_validation_verdict": result.report["manifest_validation_verdict"],
                "manifest_allowed_for_shadow_run": result.report["manifest_allowed_for_shadow_run"],
                "submit_enabled": result.report["submit_enabled"],
                "submit_allowed": result.report["submit_allowed"],
                "submit_attempted": result.report["submit_attempted"],
                "primary_blocker": result.report["primary_blocker"],
                "required_next_action": result.report["required_next_action"],
                "report_json": str(result.report_json),
            },
            sort_keys=True,
        )
    )
    return 0 if result.verdict == ShadowRunManifestVerdict.VALID else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
