"""Focused Pattern Engine v1 mining for pause-family subclusters."""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

PAUSE_FAMILIES = ("pause_pullback_resume_long", "pause_rebound_resume_short")
EXCLUDED_SESSION_PHASES = {"UNCLASSIFIED"}


@dataclass(frozen=True)
class PauseFamilyMatchRow:
    family_name: str
    direction: str
    session_phase: str
    primitive_signature: str
    move_10bar: Decimal
    mfe_20bar: Decimal
    mae_20bar: Decimal


def build_and_write_pattern_engine_v1_pause_family_mining(*, detail_csv_path: Path) -> dict[str, str]:
    rows = _load_rows(detail_csv_path)
    family_rows = _build_family_rows(rows)
    cluster_rows = _build_cluster_rows(rows)
    summary_payload = _build_summary(rows, family_rows, cluster_rows)

    prefix = Path(str(detail_csv_path).removesuffix(".pattern_engine_v1_detail.csv"))
    family_summary_path = prefix.with_suffix(".pattern_engine_v1_pause_family_summary.csv")
    cluster_summary_path = prefix.with_suffix(".pattern_engine_v1_pause_family_clusters.csv")
    summary_json_path = prefix.with_suffix(".pattern_engine_v1_pause_family_summary.json")

    _write_csv(family_summary_path, family_rows)
    _write_csv(cluster_summary_path, cluster_rows)
    summary_json_path.write_text(json.dumps(summary_payload, indent=2, sort_keys=True, default=str), encoding="utf-8")

    return {
        "pattern_engine_v1_pause_family_summary_path": str(family_summary_path),
        "pattern_engine_v1_pause_family_clusters_path": str(cluster_summary_path),
        "pattern_engine_v1_pause_family_summary_json_path": str(summary_json_path),
    }


def _load_rows(detail_csv_path: Path) -> list[PauseFamilyMatchRow]:
    rows: list[PauseFamilyMatchRow] = []
    with detail_csv_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["family_name"] not in PAUSE_FAMILIES:
                continue
            if row["session_phase"] in EXCLUDED_SESSION_PHASES:
                continue
            rows.append(
                PauseFamilyMatchRow(
                    family_name=row["family_name"],
                    direction=row["direction"],
                    session_phase=row["session_phase"],
                    primitive_signature=row["primitive_signature"],
                    move_10bar=Decimal(row["move_10bar"]),
                    mfe_20bar=Decimal(row["mfe_20bar"]),
                    mae_20bar=Decimal(row["mae_20bar"]),
                )
            )
    return rows


def _build_family_rows(rows: list[PauseFamilyMatchRow]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[PauseFamilyMatchRow]] = defaultdict(list)
    for row in rows:
        grouped[(row.family_name, row.direction, row.session_phase)].append(row)

    output: list[dict[str, Any]] = []
    for (family_name, direction, session_phase), bucket in grouped.items():
        signature_counts: dict[str, int] = defaultdict(int)
        for row in bucket:
            signature_counts[row.primitive_signature] += 1
        dominant_signature, dominant_count = max(signature_counts.items(), key=lambda item: item[1])
        output.append(
            {
                "family_name": family_name,
                "direction": direction,
                "session_phase": session_phase,
                "match_count": len(bucket),
                "estimated_value_mfe20": str(sum((row.mfe_20bar for row in bucket), Decimal("0"))),
                "avg_move_10bar": str(_avg(row.move_10bar for row in bucket)),
                "avg_mfe_20bar": str(_avg(row.mfe_20bar for row in bucket)),
                "avg_mae_20bar": str(_avg(row.mae_20bar for row in bucket)),
                "favorable_move10_rate": str(_ratio(sum(1 for row in bucket if row.move_10bar > 0), len(bucket))),
                "mfe_mae_ratio": str(_safe_ratio(_avg(row.mfe_20bar for row in bucket), _avg(row.mae_20bar for row in bucket))),
                "dominant_primitive_signature": dominant_signature,
                "dominant_signature_share": str(_ratio(dominant_count, len(bucket))),
            }
        )
    output.sort(key=lambda row: (Decimal(row["estimated_value_mfe20"]), Decimal(row["dominant_signature_share"])), reverse=True)
    return output


def _build_cluster_rows(rows: list[PauseFamilyMatchRow]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str, str], list[PauseFamilyMatchRow]] = defaultdict(list)
    family_session_counts: dict[tuple[str, str, str], int] = defaultdict(int)
    for row in rows:
        grouped[(row.family_name, row.direction, row.session_phase, row.primitive_signature)].append(row)
        family_session_counts[(row.family_name, row.direction, row.session_phase)] += 1

    output: list[dict[str, Any]] = []
    for (family_name, direction, session_phase, primitive_signature), bucket in grouped.items():
        family_session_count = family_session_counts[(family_name, direction, session_phase)]
        avg_mfe = _avg(row.mfe_20bar for row in bucket)
        avg_mae = _avg(row.mae_20bar for row in bucket)
        signature_share = _ratio(len(bucket), family_session_count)
        ranking_score = sum((row.mfe_20bar for row in bucket), Decimal("0")) * signature_share
        output.append(
            {
                "family_name": family_name,
                "direction": direction,
                "session_phase": session_phase,
                "primitive_signature": primitive_signature,
                "match_count": len(bucket),
                "signature_share_within_family_session": str(signature_share),
                "estimated_value_mfe20": str(sum((row.mfe_20bar for row in bucket), Decimal("0"))),
                "avg_move_10bar": str(_avg(row.move_10bar for row in bucket)),
                "avg_mfe_20bar": str(avg_mfe),
                "avg_mae_20bar": str(avg_mae),
                "favorable_move10_rate": str(_ratio(sum(1 for row in bucket if row.move_10bar > 0), len(bucket))),
                "mfe_mae_ratio": str(_safe_ratio(avg_mfe, avg_mae)),
                "ranking_score": str(ranking_score),
                "promotion_risk": _promotion_risk(
                    match_count=len(bucket),
                    favorable_rate=_ratio(sum(1 for row in bucket if row.move_10bar > 0), len(bucket)),
                    mfe_mae_ratio=_safe_ratio(avg_mfe, avg_mae),
                ),
            }
        )
    output.sort(key=lambda row: (Decimal(row["ranking_score"]), Decimal(row["estimated_value_mfe20"])), reverse=True)
    return output


def _build_summary(
    rows: list[PauseFamilyMatchRow],
    family_rows: list[dict[str, Any]],
    cluster_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    top_pullback = [row for row in cluster_rows if row["family_name"] == "pause_pullback_resume_long"][:10]
    top_rebound = [row for row in cluster_rows if row["family_name"] == "pause_rebound_resume_short"][:10]
    promoteable = [row for row in cluster_rows if row["promotion_risk"] == "candidate"][:8]
    noise = [row for row in cluster_rows if row["promotion_risk"] == "broad_or_noisy"][:8]
    best = cluster_rows[0] if cluster_rows else None
    return {
        "pause_family_count": len(rows),
        "families_scanned": list(PAUSE_FAMILIES),
        "session_phase_rankings": family_rows[:20],
        "pause_pullback_resume_long_rankings": top_pullback,
        "pause_rebound_resume_short_rankings": top_rebound,
        "best_candidate_cluster": best,
        "promotable_candidate_clusters": promoteable,
        "broad_or_noisy_clusters": noise,
    }


def _promotion_risk(*, match_count: int, favorable_rate: Decimal, mfe_mae_ratio: Decimal) -> str:
    if match_count < 12 or favorable_rate < Decimal("0.50") or mfe_mae_ratio < Decimal("1.05"):
        return "broad_or_noisy"
    return "candidate"


def _avg(values) -> Decimal:
    values = list(values)
    if not values:
        return Decimal("0")
    return sum(values, Decimal("0")) / Decimal(len(values))


def _ratio(numerator: int, denominator: int) -> Decimal:
    if denominator == 0:
        return Decimal("0")
    return Decimal(numerator) / Decimal(denominator)


def _safe_ratio(numerator: Decimal, denominator: Decimal) -> Decimal:
    if denominator == 0:
        return Decimal("0")
    return numerator / denominator


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        rows = [{"status": "no_rows"}]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("detail_csv_path", type=Path)
    args = parser.parse_args()

    outputs = build_and_write_pattern_engine_v1_pause_family_mining(detail_csv_path=args.detail_csv_path)
    print(json.dumps(outputs, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
