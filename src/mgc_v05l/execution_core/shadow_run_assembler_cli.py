"""CLI wrapper for Track B no-submit shadow run assembly."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .shadow_run_assembler import DEFAULT_SHADOW_RUN_OUTPUT_ROOT, ShadowRunAssemblerVerdict, assemble_shadow_run


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Assemble a Track B no-submit shadow run from manifest, registry, and intent JSON files.")
    parser.add_argument("--manifest-json", required=True, type=Path)
    parser.add_argument("--registry-json", required=True, type=Path)
    parser.add_argument("--intent-json", action="append", type=Path, default=[])
    parser.add_argument("--readiness-summary-json", type=Path)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_SHADOW_RUN_OUTPUT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    manifest_payload = json.loads(args.manifest_json.read_text(encoding="utf-8"))
    registry_payload = json.loads(args.registry_json.read_text(encoding="utf-8"))
    intent_payloads = [json.loads(path.read_text(encoding="utf-8")) for path in args.intent_json]
    result = assemble_shadow_run(
        manifest_payload=manifest_payload,
        registry_payload=registry_payload,
        intent_payloads=intent_payloads,
        readiness_summary_json=args.readiness_summary_json,
        output_root=args.output_root,
    )
    print(
        json.dumps(
            {
                "shadow_run_verdict": result.report["shadow_run_verdict"],
                "manifest_validation_verdict": result.report["manifest_validation_verdict"],
                "total_intents": result.report["total_intents"],
                "blocked_count": result.report["blocked_count"],
                "submit_allowed": result.report["submit_allowed"],
                "submit_attempted": result.report["submit_attempted"],
                "primary_blocker": result.report["primary_blocker"],
                "required_next_action": result.report["required_next_action"],
                "report_json": str(result.report_json),
            },
            sort_keys=True,
        )
    )
    return 0 if result.verdict == ShadowRunAssemblerVerdict.ASSEMBLED_FOR_REVIEW else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
