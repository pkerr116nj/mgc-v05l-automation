"""Focused Pattern Engine v1 lane selection for failed_move_reversal."""

from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

FAMILY_NAME = "failed_move_reversal"
EXCLUDED_SESSION_PHASES = {"UNCLASSIFIED"}


@dataclass(frozen=True)
class FailedMoveReversalRow:
    direction: str
    session_phase: str
    primitive_signature: str
    move_10bar: Decimal
    mfe_20bar: Decimal
    mae_20bar: Decimal

    @property
    def directional_move_10bar(self) -> Decimal:
        return self.move_10bar if self.direction == "LONG" else -self.move_10bar


def build_and_write_pattern_engine_v1_failed_move_reversal_lane_mining(*, detail_csv_path: Path) -> dict[str, str]:
    rows = _load_rows(detail_csv_path)
    lane_rows = _build_lane_rows(rows)
    cluster_rows = _build_cluster_rows(rows)
    summary_payload = _build_summary(lane_rows, cluster_rows)

    prefix = Path(str(detail_csv_path).removesuffix(".pattern_engine_v1_detail.csv"))
    lane_summary_path = prefix.with_suffix(".pattern_engine_v1_failed_move_reversal_lane_summary.csv")
    cluster_summary_path = prefix.with_suffix(".pattern_engine_v1_failed_move_reversal_lane_clusters.csv")
    summary_json_path = prefix.with_suffix(".pattern_engine_v1_failed_move_reversal_lane_summary.json")

    _write_csv(lane_summary_path, lane_rows)
    _write_csv(cluster_summary_path, cluster_rows)
    summary_json_path.write_text(json.dumps(summary_payload, indent=2, sort_keys=True, default=str), encoding="utf-8")

    return {
        "pattern_engine_v1_failed_move_reversal_lane_summary_path": str(lane_summary_path),
        "pattern_engine_v1_failed_move_reversal_lane_clusters_path": str(cluster_summary_path),
        "pattern_engine_v1_failed_move_reversal_lane_summary_json_path": str(summary_json_path),
    }


def _load_rows(detail_csv_path: Path) -> list[FailedMoveReversalRow]:
    rows: list[FailedMoveReversalRow] = []
    with detail_csv_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["family_name"] != FAMILY_NAME:
                continue
            if row["session_phase"] in EXCLUDED_SESSION_PHASES:
                continue
            rows.append(
                FailedMoveReversalRow(
                    direction=row["direction"],
                    session_phase=row["session_phase"],
                    primitive_signature=row["primitive_signature"],
                    move_10bar=Decimal(row["move_10bar"]),
                    mfe_20bar=Decimal(row["mfe_20bar"]),
                    mae_20bar=Decimal(row["mae_20bar"]),
                )
            )
    return rows


def _build_lane_rows(rows: list[FailedMoveReversalRow]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[FailedMoveReversalRow]] = defaultdict(list)
    for row in rows:
        grouped[(row.session_phase, row.direction)].append(row)

    output: list[dict[str, Any]] = []
    for (session_phase, direction), bucket in grouped.items():
        signature_counts = Counter(row.primitive_signature for row in bucket)
        top_signatures = signature_counts.most_common(3)
        dominant_signature, dominant_count = top_signatures[0]
        avg_mfe = _avg(row.mfe_20bar for row in bucket)
        avg_mae = _avg(row.mae_20bar for row in bucket)
        directional_avg = _avg(row.directional_move_10bar for row in bucket)
        aligned_rate = _ratio(sum(1 for row in bucket if row.directional_move_10bar > 0), len(bucket))
        top3_share = _ratio(sum(count for _, count in top_signatures), len(bucket))
        mfe_mae_ratio = _safe_ratio(avg_mfe, avg_mae)
        output.append(
            {
                "family_name": FAMILY_NAME,
                "session_phase": session_phase,
                "direction": direction,
                "match_count": len(bucket),
                "estimated_value_mfe20": str(sum((row.mfe_20bar for row in bucket), Decimal("0"))),
                "avg_move_10bar": str(_avg(row.move_10bar for row in bucket)),
                "directional_avg_move_10bar": str(directional_avg),
                "aligned_move10_rate": str(aligned_rate),
                "avg_mfe_20bar": str(avg_mfe),
                "avg_mae_20bar": str(avg_mae),
                "mfe_mae_ratio": str(mfe_mae_ratio),
                "dominant_primitive_signature": dominant_signature,
                "dominant_signature_share": str(_ratio(dominant_count, len(bucket))),
                "top3_signature_share": str(top3_share),
                "coherence_label": _coherence_label(
                    match_count=len(bucket),
                    directional_avg_move=directional_avg,
                    aligned_rate=aligned_rate,
                    mfe_mae_ratio=mfe_mae_ratio,
                    top3_signature_share=top3_share,
                ),
                "separator_followup_warranted": _separator_followup_warranted(
                    match_count=len(bucket),
                    directional_avg_move=directional_avg,
                    aligned_rate=aligned_rate,
                    mfe_mae_ratio=mfe_mae_ratio,
                ),
                "lane_selection_score": str(
                    _lane_selection_score(
                        estimated_value=sum((row.mfe_20bar for row in bucket), Decimal("0")),
                        directional_avg_move=directional_avg,
                        aligned_rate=aligned_rate,
                        mfe_mae_ratio=mfe_mae_ratio,
                        top3_signature_share=top3_share,
                    )
                ),
            }
        )
    output.sort(key=lambda row: Decimal(row["lane_selection_score"]), reverse=True)
    return output


def _build_cluster_rows(rows: list[FailedMoveReversalRow]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[FailedMoveReversalRow]] = defaultdict(list)
    lane_counts: dict[tuple[str, str], int] = defaultdict(int)
    for row in rows:
        grouped[(row.session_phase, row.direction, row.primitive_signature)].append(row)
        lane_counts[(row.session_phase, row.direction)] += 1

    output: list[dict[str, Any]] = []
    for (session_phase, direction, primitive_signature), bucket in grouped.items():
        lane_count = lane_counts[(session_phase, direction)]
        avg_mfe = _avg(row.mfe_20bar for row in bucket)
        avg_mae = _avg(row.mae_20bar for row in bucket)
        directional_avg = _avg(row.directional_move_10bar for row in bucket)
        aligned_rate = _ratio(sum(1 for row in bucket if row.directional_move_10bar > 0), len(bucket))
        mfe_mae_ratio = _safe_ratio(avg_mfe, avg_mae)
        output.append(
            {
                "family_name": FAMILY_NAME,
                "session_phase": session_phase,
                "direction": direction,
                "primitive_signature": primitive_signature,
                "match_count": len(bucket),
                "lane_signature_share": str(_ratio(len(bucket), lane_count)),
                "estimated_value_mfe20": str(sum((row.mfe_20bar for row in bucket), Decimal("0"))),
                "directional_avg_move_10bar": str(directional_avg),
                "aligned_move10_rate": str(aligned_rate),
                "avg_mfe_20bar": str(avg_mfe),
                "avg_mae_20bar": str(avg_mae),
                "mfe_mae_ratio": str(mfe_mae_ratio),
                "cluster_label": _cluster_label(
                    match_count=len(bucket),
                    directional_avg_move=directional_avg,
                    aligned_rate=aligned_rate,
                    mfe_mae_ratio=mfe_mae_ratio,
                ),
            }
        )
    output.sort(key=lambda row: (Decimal(row["estimated_value_mfe20"]), Decimal(row["aligned_move10_rate"])), reverse=True)
    return output


def _build_summary(lane_rows: list[dict[str, Any]], cluster_rows: list[dict[str, Any]]) -> dict[str, Any]:
    recommended = lane_rows[0] if lane_rows else None
    worthwhile = [row for row in lane_rows if row["separator_followup_warranted"] == "yes"][:10]
    broad = [row for row in lane_rows if row["coherence_label"] == "likely_too_broad"][:10]
    diffuse = [row for row in lane_rows if row["coherence_label"] == "diffuse"][:10]
    top_clusters = cluster_rows[:15]
    return {
        "family_name": FAMILY_NAME,
        "lane_count": len(lane_rows),
        "recommended_next_lane": recommended,
        "worthwhile_separator_followups": worthwhile,
        "likely_too_broad_lanes": broad,
        "diffuse_lanes": diffuse,
        "top_signature_clusters": top_clusters,
    }


def _coherence_label(
    *,
    match_count: int,
    directional_avg_move: Decimal,
    aligned_rate: Decimal,
    mfe_mae_ratio: Decimal,
    top3_signature_share: Decimal,
) -> str:
    if match_count < 60:
        return "too_thin"
    if directional_avg_move <= 0 or mfe_mae_ratio < Decimal("1.00"):
        return "likely_too_broad"
    if aligned_rate >= Decimal("0.54") and mfe_mae_ratio >= Decimal("1.12") and top3_signature_share >= Decimal("0.18"):
        return "coherent"
    if aligned_rate >= Decimal("0.50") and mfe_mae_ratio >= Decimal("1.05"):
        return "diffuse"
    return "likely_too_broad"


def _separator_followup_warranted(
    *,
    match_count: int,
    directional_avg_move: Decimal,
    aligned_rate: Decimal,
    mfe_mae_ratio: Decimal,
) -> str:
    if match_count < 80:
        return "no"
    if directional_avg_move <= 0:
        return "no"
    if aligned_rate < Decimal("0.50"):
        return "no"
    if mfe_mae_ratio < Decimal("1.05"):
        return "no"
    return "yes"


def _cluster_label(
    *,
    match_count: int,
    directional_avg_move: Decimal,
    aligned_rate: Decimal,
    mfe_mae_ratio: Decimal,
) -> str:
    if match_count < 12:
        return "too_thin"
    if directional_avg_move > 0 and aligned_rate >= Decimal("0.55") and mfe_mae_ratio >= Decimal("1.10"):
        return "candidate"
    return "broad_or_noisy"


def _lane_selection_score(
    *,
    estimated_value: Decimal,
    directional_avg_move: Decimal,
    aligned_rate: Decimal,
    mfe_mae_ratio: Decimal,
    top3_signature_share: Decimal,
) -> Decimal:
    directional_component = max(directional_avg_move, Decimal("0"))
    quality_component = max(mfe_mae_ratio - Decimal("0.90"), Decimal("0"))
    return estimated_value * directional_component * aligned_rate * quality_component * max(top3_signature_share, Decimal("0.05"))


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

    outputs = build_and_write_pattern_engine_v1_failed_move_reversal_lane_mining(detail_csv_path=args.detail_csv_path)
    print(json.dumps(outputs, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
