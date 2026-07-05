"""CLI for the Canonical Investigation Engine."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_canonical_investigation_engine import (
    DEFAULT_OUTPUT_DIR,
    attach_reference,
    create_investigation,
    list_investigations,
    load_investigation,
    publish_ie1_artifacts,
    summarize_investigation,
    validate_investigation,
    write_investigation,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage diagnostic Canonical Investigation records.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    subparsers = parser.add_subparsers(dest="command", required=True)

    create = subparsers.add_parser("create")
    create.add_argument("--title", required=True)
    create.add_argument("--description", required=True)
    create.add_argument("--hypothesis", required=True)
    create.add_argument("--owner", default="operator")
    create.add_argument("--tag", action="append", default=[])
    create.add_argument("--investigation-id")
    create.add_argument("--now")

    attach = subparsers.add_parser("attach")
    attach.add_argument("--investigation-id", required=True)
    attach.add_argument("--reference-type", required=True)
    attach.add_argument("--reference-id", required=True)
    attach.add_argument("--artifact-path")
    attach.add_argument("--label")
    attach.add_argument("--now")

    subparsers.add_parser("list")

    show = subparsers.add_parser("show")
    show.add_argument("--investigation-id", required=True)

    validate = subparsers.add_parser("validate")
    validate.add_argument("--investigation-id", required=True)

    export = subparsers.add_parser("export-summary")
    export.add_argument("--investigation-id", required=True)

    publish = subparsers.add_parser("publish-ie1-artifacts")
    publish.add_argument("--now")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "create":
        result = create_investigation(
            title=args.title,
            description=args.description,
            hypothesis=args.hypothesis,
            owner=args.owner,
            tags=args.tag,
            investigation_id=args.investigation_id,
            now=args.now,
            output_dir=args.output_dir,
        )
        _print({"investigation_id": result.investigation["investigation_id"], "path": str(result.investigation_path), "summary_path": str(result.summary_path)})
        return 0
    if args.command == "attach":
        investigation = load_investigation(args.investigation_id, output_dir=args.output_dir)
        updated = attach_reference(
            investigation,
            reference_type=args.reference_type,
            reference_id=args.reference_id,
            artifact_path=args.artifact_path,
            label=args.label,
            now=args.now,
        )
        result = write_investigation(updated, output_dir=args.output_dir)
        _print({"investigation_id": result.investigation["investigation_id"], "reference_count": result.summary["reference_count"], "timeline_event_count": result.summary["timeline_event_count"]})
        return 0
    if args.command == "list":
        _print({"investigations": list_investigations(output_dir=args.output_dir)})
        return 0
    if args.command == "show":
        _print(load_investigation(args.investigation_id, output_dir=args.output_dir))
        return 0
    if args.command == "validate":
        investigation = load_investigation(args.investigation_id, output_dir=args.output_dir)
        _print(validate_investigation(investigation))
        return 0
    if args.command == "export-summary":
        investigation = load_investigation(args.investigation_id, output_dir=args.output_dir)
        _print(summarize_investigation(investigation))
        return 0
    if args.command == "publish-ie1-artifacts":
        _print(publish_ie1_artifacts(output_dir=args.output_dir, now=args.now))
        return 0
    raise AssertionError(f"Unhandled command: {args.command}")


def _print(payload: object) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
