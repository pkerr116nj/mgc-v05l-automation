"""CLI for the Canonical Evidence Engine."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_canonical_evidence_engine import (
    DEFAULT_OUTPUT_DIR,
    add_evidence_relationship,
    attach_evidence_reference,
    create_evidence,
    list_evidence,
    load_evidence,
    publish_ie2_artifacts,
    summarize_evidence,
    update_evidence_status,
    validate_evidence,
    write_evidence,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage deterministic Canonical Evidence records.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    subparsers = parser.add_subparsers(dest="command", required=True)

    create = subparsers.add_parser("create")
    create.add_argument("--investigation-id", required=True)
    create.add_argument("--title", required=True)
    create.add_argument("--summary", required=True)
    create.add_argument("--classification", required=True)
    create.add_argument("--evidence-type", required=True)
    create.add_argument("--source-component", required=True)
    create.add_argument("--evidence-id")
    create.add_argument("--now")

    attach = subparsers.add_parser("attach")
    attach.add_argument("--evidence-id", required=True)
    attach.add_argument("--attachment-type", required=True)
    attach.add_argument("--reference-id", required=True)
    attach.add_argument("--artifact-path")
    attach.add_argument("--label")
    attach.add_argument("--now")

    relationship = subparsers.add_parser("relationship")
    relationship.add_argument("--evidence-id", required=True)
    relationship.add_argument("--relationship-type", required=True)
    relationship.add_argument("--target-evidence-id", required=True)
    relationship.add_argument("--now")

    status = subparsers.add_parser("status")
    status.add_argument("--evidence-id", required=True)
    status.add_argument("--status", required=True)
    status.add_argument("--now")

    list_cmd = subparsers.add_parser("list")
    list_cmd.add_argument("--investigation-id")

    show = subparsers.add_parser("show")
    show.add_argument("--evidence-id", required=True)

    validate = subparsers.add_parser("validate")
    validate.add_argument("--evidence-id", required=True)

    export = subparsers.add_parser("export-summary")
    export.add_argument("--evidence-id", required=True)

    publish = subparsers.add_parser("publish-ie2-artifacts")
    publish.add_argument("--now")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "create":
        result = create_evidence(
            investigation_id=args.investigation_id,
            evidence_id=args.evidence_id,
            title=args.title,
            summary=args.summary,
            evidence_classification=args.classification,
            evidence_type=args.evidence_type,
            source_component=args.source_component,
            now=args.now,
            output_dir=args.output_dir,
        )
        _print({"evidence_id": result.evidence["evidence_id"], "path": str(result.evidence_path), "summary_path": str(result.summary_path)})
        return 0
    if args.command == "attach":
        evidence = load_evidence(args.evidence_id, output_dir=args.output_dir)
        updated = attach_evidence_reference(
            evidence,
            attachment_type=args.attachment_type,
            reference_id=args.reference_id,
            artifact_path=args.artifact_path,
            label=args.label,
            now=args.now,
        )
        result = write_evidence(updated, output_dir=args.output_dir)
        _print({"evidence_id": result.evidence["evidence_id"], "attachment_count": result.summary["attachment_count"]})
        return 0
    if args.command == "relationship":
        evidence = load_evidence(args.evidence_id, output_dir=args.output_dir)
        updated = add_evidence_relationship(evidence, relationship_type=args.relationship_type, target_evidence_id=args.target_evidence_id, now=args.now)
        result = write_evidence(updated, output_dir=args.output_dir)
        _print({"evidence_id": result.evidence["evidence_id"], "relationship_count": result.summary["relationship_count"]})
        return 0
    if args.command == "status":
        evidence = load_evidence(args.evidence_id, output_dir=args.output_dir)
        result = write_evidence(update_evidence_status(evidence, status=args.status, now=args.now), output_dir=args.output_dir)
        _print({"evidence_id": result.evidence["evidence_id"], "status": result.evidence["status"]})
        return 0
    if args.command == "list":
        _print({"evidence": list_evidence(output_dir=args.output_dir, investigation_id=args.investigation_id)})
        return 0
    if args.command == "show":
        _print(load_evidence(args.evidence_id, output_dir=args.output_dir))
        return 0
    if args.command == "validate":
        _print(validate_evidence(load_evidence(args.evidence_id, output_dir=args.output_dir)))
        return 0
    if args.command == "export-summary":
        _print(summarize_evidence(load_evidence(args.evidence_id, output_dir=args.output_dir)))
        return 0
    if args.command == "publish-ie2-artifacts":
        _print(publish_ie2_artifacts(output_dir=args.output_dir, now=args.now))
        return 0
    raise AssertionError(f"Unhandled command: {args.command}")


def _print(payload: object) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
