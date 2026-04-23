"""Tight validation ladder for the best ES overnight candidate."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from .es_mes_asia_london_volatility_floor_research import run_es_mes_asia_london_volatility_floor_research


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "es_asia_london_candidate_validation_v1"
DEFAULT_FLOORS: tuple[float, ...] = (1.0, 1.25, 1.5, 1.75)
DEFAULT_LONG_VARIANTS: tuple[str, ...] = (
    "segment_forced_long_v6_contextual_fallback",
    "segment_forced_long_v5_dip_reclaim_or_bar8",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="es-asia-london-candidate-validation")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--config", action="append", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = run_es_asia_london_candidate_validation(
        start_date=args.start_date,
        end_date=args.end_date,
        output_dir=args.output_dir,
        config_paths=args.config,
    )
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0


def run_es_asia_london_candidate_validation(
    *,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    output_dir: str | Path | None = None,
    config_paths: list[str] | list[Path] | tuple[str | Path, ...] | None = None,
) -> dict[str, Any]:
    resolved_output_dir = Path(output_dir or DEFAULT_OUTPUT_DIR).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    base = run_es_mes_asia_london_volatility_floor_research(
        symbols=("ES",),
        start_date=start_date,
        end_date=end_date,
        output_dir=resolved_output_dir,
        config_paths=config_paths,
        volatility_floors=DEFAULT_FLOORS,
        long_variant_ids=DEFAULT_LONG_VARIANTS,
        short_variant_ids=(),
    )
    json_path = Path(base["artifact_paths"]["json"])
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    es_rows = []
    for key, value in payload["symbol_reports"]["ES"]["variants"].items():
        summary = value["trade_summary"]
        es_rows.append(
            {
                "variant_key": key,
                "variant_id": value["variant_id"],
                "gate_mode": value["gate_mode"],
                "volatility_floor_ratio": value["volatility_floor_ratio"],
                "average_net_pnl_points": summary["average_net_pnl_points"],
                "net_profit_factor": summary["net_profit_factor"],
                "entered_trade_count": summary["entered_trade_count"],
                "max_drawdown_points": summary["max_drawdown_points"],
                "total_net_pnl_points": summary["total_net_pnl_points"],
            }
        )
    es_rows.sort(
        key=lambda row: (
            float(row["average_net_pnl_points"] or 0.0),
            float(row["net_profit_factor"] or 0.0),
            -float(row["max_drawdown_points"] or 0.0),
            int(row["entered_trade_count"]),
        ),
        reverse=True,
    )
    validation_payload: dict[str, Any] = {
        "generated_at": datetime.now(UTC).isoformat(),
        "study_id": "es_asia_london_candidate_validation",
        "definition": {
            "symbol": "ES",
            "volatility_floors": list(DEFAULT_FLOORS),
            "long_variants": list(DEFAULT_LONG_VARIANTS),
            "source_study": "es_mes_asia_london_volatility_floor_research",
            "start_date": start_date.isoformat() if isinstance(start_date, date) else start_date,
            "end_date": end_date.isoformat() if isinstance(end_date, date) else end_date,
        },
        "artifact_paths": {"base_json": str(json_path)},
        "ranked_candidates": es_rows,
    }
    validation_path = resolved_output_dir / "es_asia_london_candidate_validation.json"
    validation_path.write_text(json.dumps(validation_payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return {"artifact_paths": {"json": str(validation_path), "base_json": str(json_path)}, "ranked_candidates": es_rows[:8]}


if __name__ == "__main__":
    raise SystemExit(main())
