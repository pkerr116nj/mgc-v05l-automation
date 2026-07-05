"""CLI for the Canonical Conclusions Engine."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_canonical_claims_engine import DEFAULT_OUTPUT_DIR as DEFAULT_CLAIMS_DIR
from mgc_v05l.execution_core.track_b_canonical_claims_engine import load_claim
from mgc_v05l.execution_core.track_b_canonical_conclusions_engine import (
    DEFAULT_OUTPUT_DIR,
    attach_contradicting_claim,
    attach_supporting_claim,
    create_conclusion,
    list_conclusions,
    load_conclusion,
    publish_ie4_artifacts,
    refresh_conclusion_validation,
    summarize_conclusion,
    validate_conclusion,
    write_conclusion,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage deterministic Canonical Conclusion records.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--claims-dir", type=Path, default=DEFAULT_CLAIMS_DIR)
    subparsers = parser.add_subparsers(dest="command", required=True)

    create = subparsers.add_parser("create")
    create.add_argument("--investigation-id", required=True)
    create.add_argument("--title", required=True)
    create.add_argument("--hypothesis", required=True)
    create.add_argument("--conclusion", required=True)
    create.add_argument("--rationale", required=True)
    create.add_argument("--classification", required=True)
    create.add_argument("--confidence", default="UNKNOWN")
    create.add_argument("--conclusion-id")
    create.add_argument("--now")

    for name in ("attach-support", "attach-contradiction"):
        attach = subparsers.add_parser(name)
        attach.add_argument("--conclusion-id", required=True)
        attach.add_argument("--claim-id", required=True)
        attach.add_argument("--now")

    validate = subparsers.add_parser("validate")
    validate.add_argument("--conclusion-id", required=True)

    list_cmd = subparsers.add_parser("list")
    list_cmd.add_argument("--investigation-id")

    show = subparsers.add_parser("show")
    show.add_argument("--conclusion-id", required=True)

    export = subparsers.add_parser("export-summary")
    export.add_argument("--conclusion-id", required=True)

    publish = subparsers.add_parser("publish-ie4-artifacts")
    publish.add_argument("--now")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "create":
        result = create_conclusion(
            investigation_id=args.investigation_id,
            conclusion_id=args.conclusion_id,
            title=args.title,
            hypothesis=args.hypothesis,
            conclusion=args.conclusion,
            rationale=args.rationale,
            conclusion_classification=args.classification,
            confidence=args.confidence,
            now=args.now,
            output_dir=args.output_dir,
        )
        _print({"conclusion_id": result.conclusion["conclusion_id"], "path": str(result.conclusion_path), "summary_path": str(result.summary_path)})
        return 0
    if args.command in {"attach-support", "attach-contradiction"}:
        conclusion = load_conclusion(args.conclusion_id, output_dir=args.output_dir)
        claim = load_claim(args.claim_id, output_dir=args.claims_dir)
        updated = (
            attach_supporting_claim(conclusion, claim, now=args.now)
            if args.command == "attach-support"
            else attach_contradicting_claim(conclusion, claim, now=args.now)
        )
        result = write_conclusion(updated, output_dir=args.output_dir)
        _print({"conclusion_id": result.conclusion["conclusion_id"], "outcome": result.summary["outcome"], "confidence": result.summary["confidence"]})
        return 0
    if args.command == "validate":
        conclusion = refresh_conclusion_validation(load_conclusion(args.conclusion_id, output_dir=args.output_dir))
        result = write_conclusion(conclusion, output_dir=args.output_dir)
        _print({"conclusion_id": result.conclusion["conclusion_id"], "validation": result.conclusion["validation"]})
        return 0
    if args.command == "list":
        _print({"conclusions": list_conclusions(output_dir=args.output_dir, investigation_id=args.investigation_id)})
        return 0
    if args.command == "show":
        _print(load_conclusion(args.conclusion_id, output_dir=args.output_dir))
        return 0
    if args.command == "export-summary":
        _print(summarize_conclusion(load_conclusion(args.conclusion_id, output_dir=args.output_dir)))
        return 0
    if args.command == "publish-ie4-artifacts":
        _print(publish_ie4_artifacts(output_dir=args.output_dir, now=args.now))
        return 0
    raise AssertionError(f"Unhandled command: {args.command}")


def _print(payload: object) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
