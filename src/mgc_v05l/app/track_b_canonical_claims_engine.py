"""CLI for the Canonical Claims Engine."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_canonical_claims_engine import (
    DEFAULT_OUTPUT_DIR,
    attach_contradicting_evidence,
    attach_supporting_evidence,
    create_claim,
    list_claims,
    load_claim,
    publish_ie3_artifacts,
    refresh_claim_validation,
    summarize_claim,
    validate_claim,
    write_claim,
)
from mgc_v05l.execution_core.track_b_canonical_evidence_engine import DEFAULT_OUTPUT_DIR as DEFAULT_EVIDENCE_DIR
from mgc_v05l.execution_core.track_b_canonical_evidence_engine import load_evidence


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage deterministic Canonical Claim records.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--evidence-dir", type=Path, default=DEFAULT_EVIDENCE_DIR)
    subparsers = parser.add_subparsers(dest="command", required=True)

    create = subparsers.add_parser("create")
    create.add_argument("--investigation-id", required=True)
    create.add_argument("--title", required=True)
    create.add_argument("--statement", required=True)
    create.add_argument("--rationale", required=True)
    create.add_argument("--classification", required=True)
    create.add_argument("--claim-type", required=True)
    create.add_argument("--confidence", default="UNKNOWN")
    create.add_argument("--claim-id")
    create.add_argument("--now")

    for name in ("attach-support", "attach-contradiction"):
        attach = subparsers.add_parser(name)
        attach.add_argument("--claim-id", required=True)
        attach.add_argument("--evidence-id", required=True)
        attach.add_argument("--now")

    validate = subparsers.add_parser("validate")
    validate.add_argument("--claim-id", required=True)

    list_cmd = subparsers.add_parser("list")
    list_cmd.add_argument("--investigation-id")

    show = subparsers.add_parser("show")
    show.add_argument("--claim-id", required=True)

    export = subparsers.add_parser("export-summary")
    export.add_argument("--claim-id", required=True)

    publish = subparsers.add_parser("publish-ie3-artifacts")
    publish.add_argument("--now")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "create":
        result = create_claim(
            investigation_id=args.investigation_id,
            claim_id=args.claim_id,
            title=args.title,
            statement=args.statement,
            rationale=args.rationale,
            claim_classification=args.classification,
            claim_type=args.claim_type,
            confidence=args.confidence,
            now=args.now,
            output_dir=args.output_dir,
        )
        _print({"claim_id": result.claim["claim_id"], "path": str(result.claim_path), "summary_path": str(result.summary_path)})
        return 0
    if args.command in {"attach-support", "attach-contradiction"}:
        claim = load_claim(args.claim_id, output_dir=args.output_dir)
        evidence = load_evidence(args.evidence_id, output_dir=args.evidence_dir)
        updated = (
            attach_supporting_evidence(claim, evidence, now=args.now)
            if args.command == "attach-support"
            else attach_contradicting_evidence(claim, evidence, now=args.now)
        )
        result = write_claim(updated, output_dir=args.output_dir)
        _print({"claim_id": result.claim["claim_id"], "validation_status": result.summary["validation_status"]})
        return 0
    if args.command == "validate":
        claim = refresh_claim_validation(load_claim(args.claim_id, output_dir=args.output_dir))
        result = write_claim(claim, output_dir=args.output_dir)
        _print({"claim_id": result.claim["claim_id"], "validation": result.claim["validation"]})
        return 0
    if args.command == "list":
        _print({"claims": list_claims(output_dir=args.output_dir, investigation_id=args.investigation_id)})
        return 0
    if args.command == "show":
        _print(load_claim(args.claim_id, output_dir=args.output_dir))
        return 0
    if args.command == "export-summary":
        _print(summarize_claim(load_claim(args.claim_id, output_dir=args.output_dir)))
        return 0
    if args.command == "publish-ie3-artifacts":
        _print(publish_ie3_artifacts(output_dir=args.output_dir, now=args.now))
        return 0
    raise AssertionError(f"Unhandled command: {args.command}")


def _print(payload: object) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
