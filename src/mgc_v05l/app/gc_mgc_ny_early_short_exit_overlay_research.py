"""Exit-overlay research for filtered GC/MGC NY-early short trades."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any

from ..research.trend_participation.models import ResearchBar
from ..research.trend_participation.storage import load_sqlite_bars, normalize_and_check_bars, resample_bars_from_1m
from .gc_mgc_ny_early_short_research import (
    DEFAULT_CONFIG_PATHS,
    DEFAULT_SYMBOLS,
    NyEarlyShortSpec,
    _build_ny_early_session_contexts,
    _contract_economics,
    _execution_cost_points,
    _find_exit,
    _load_end_timestamp,
    _load_start_timestamp,
    _parse_date,
    build_variant_specs,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SOURCE_JSON = (
    REPO_ROOT / "outputs" / "reports" / "gc_mgc_ny_early_short_archive_v1" / "gc_mgc_ny_early_short_research.json"
)
DEFAULT_META_JSON = (
    REPO_ROOT
    / "outputs"
    / "reports"
    / "gc_mgc_ny_early_short_meta_label_archive_v1"
    / "gc_mgc_ny_early_short_meta_label_research.json"
)
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "gc_mgc_ny_early_short_exit_overlay"
DEFAULT_SOURCE_VARIANTS = ("ny_early_short_v2_failed_pop_3m",)


@dataclass(frozen=True)
class ExitOverlaySpec:
    overlay_id: str
    description: str
    first_bar_max_close_r: float | None = None
    first_bar_min_mfe_r: float | None = None
    second_bar_min_mfe_r: float | None = None


@dataclass(frozen=True)
class OverlayTradeResult:
    symbol: str
    trade_date: str
    source_variant: str
    overlay_id: str
    entry_bar_number: int
    exit_bar_number: int
    exit_reason: str
    pnl_points: float
    net_pnl_points: float
    gross_r_multiple: float | None
    net_r_multiple: float | None
    mfe_points: float
    mae_points: float


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gc-mgc-ny-early-short-exit-overlay-research")
    parser.add_argument("--source-json", default=str(DEFAULT_SOURCE_JSON), help="Path to source NY-early short research JSON.")
    parser.add_argument("--meta-json", default=str(DEFAULT_META_JSON), help="Path to NY-early short meta-label JSON.")
    parser.add_argument("--source-variant", action="append", default=None, help="Source candidate variant to evaluate.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Output directory override.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = run_gc_mgc_ny_early_short_exit_overlay_research(
        source_json=Path(args.source_json),
        meta_json=Path(args.meta_json),
        source_variants=tuple(args.source_variant or DEFAULT_SOURCE_VARIANTS),
        output_dir=Path(args.output_dir),
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def run_gc_mgc_ny_early_short_exit_overlay_research(
    *,
    source_json: Path | None = None,
    meta_json: Path | None = None,
    source_variants: tuple[str, ...] | None = DEFAULT_SOURCE_VARIANTS,
    output_dir: Path | None = DEFAULT_OUTPUT_DIR,
) -> dict[str, Any]:
    resolved_source_json = Path(source_json or DEFAULT_SOURCE_JSON).resolve()
    resolved_meta_json = Path(meta_json or DEFAULT_META_JSON).resolve()
    resolved_output_dir = Path(output_dir or DEFAULT_OUTPUT_DIR).resolve()
    resolved_variants = tuple(source_variants or DEFAULT_SOURCE_VARIANTS)
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    source_report = json.loads(resolved_source_json.read_text(encoding="utf-8"))
    meta_report = json.loads(resolved_meta_json.read_text(encoding="utf-8"))
    selected_rows = _load_selected_test_rows(meta_report=meta_report)
    variant_specs = {spec.variant_id: spec for spec in build_variant_specs()}
    sqlite_path = _resolve_database_path(source_report["database_path"])
    overlays = build_overlay_specs()

    variant_reports: list[dict[str, Any]] = []
    for source_variant in resolved_variants:
        variant_selected = [row for row in selected_rows if row["source_variant"] == source_variant]
        if not variant_selected:
            variant_reports.append({"source_variant": source_variant, "status": "no_selected_test_rows"})
            continue
        spec = variant_specs[source_variant]
        selected_dates = sorted({date.fromisoformat(row["trade_date"]) for row in variant_selected})
        start_day = min(selected_dates)
        end_day = max(selected_dates)

        symbol_contexts: dict[str, dict[str, list[ResearchBar]]] = {}
        for symbol in sorted({row["symbol"] for row in variant_selected}):
            one_minute = load_sqlite_bars(
                sqlite_path=sqlite_path,
                instrument=symbol,
                timeframe="1m",
                data_source="historical_1m_canonical",
                start_ts=_load_start_timestamp(start_day),
                end_ts=_load_end_timestamp(end_day),
            )
            normalized_1m, _issues = normalize_and_check_bars(bars=one_minute, timeframe="1m")
            decision_bars = resample_bars_from_1m(bars_1m=normalized_1m, target_timeframe=spec.decision_timeframe)
            contexts = _build_ny_early_session_contexts(
                one_minute_bars=normalized_1m,
                decision_bars=decision_bars,
                start_day=start_day,
                end_day=end_day,
                max_pre_context_minutes=spec.pre_context_minutes,
            )
            symbol_contexts[symbol] = {
                context.trade_date.isoformat(): context.segment_bars
                for context in contexts
            }

        variant_sessions = {
            symbol: {
                row["trade_date"]: row
                for row in source_report["symbol_reports"][symbol]["variants"][source_variant]["sessions"]
            }
            for symbol in {row["symbol"] for row in variant_selected}
        }

        baseline_results: list[OverlayTradeResult] = []
        overlay_results_by_id: dict[str, list[OverlayTradeResult]] = {overlay.overlay_id: [] for overlay in overlays}
        for row in variant_selected:
            symbol = row["symbol"]
            trade_date = row["trade_date"]
            session = variant_sessions[symbol][trade_date]
            segment_bars = symbol_contexts[symbol][trade_date]
            if not session.get("entered"):
                continue
            baseline_results.append(_baseline_result(symbol=symbol, trade_date=trade_date, source_variant=source_variant, session=session))
            for overlay in overlays:
                overlay_results_by_id[overlay.overlay_id].append(
                    _apply_overlay(
                        symbol=symbol,
                        trade_date=trade_date,
                        source_variant=source_variant,
                        session=session,
                        segment_bars=segment_bars,
                        spec=spec,
                        overlay=overlay,
                    )
                )

        baseline_summary = _summarize_overlay_results(baseline_results)
        overlay_summaries = {
            overlay.overlay_id: {
                "description": overlay.description,
                "summary": _summarize_overlay_results(overlay_results_by_id[overlay.overlay_id]),
            }
            for overlay in overlays
        }
        variant_reports.append(
            {
                "source_variant": source_variant,
                "selected_trade_count": len(baseline_results),
                "baseline_summary": baseline_summary,
                "overlay_summaries": overlay_summaries,
            }
        )

    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "study_id": "gc_mgc_ny_early_short_exit_overlay",
        "source_json": str(resolved_source_json),
        "meta_json": str(resolved_meta_json),
        "source_variants": list(resolved_variants),
        "overlay_specs": [asdict(spec) for spec in overlays],
        "variant_reports": variant_reports,
    }
    json_path = resolved_output_dir / "gc_mgc_ny_early_short_exit_overlay_research.json"
    markdown_path = resolved_output_dir / "gc_mgc_ny_early_short_exit_overlay_research.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {
        "mode": "gc_mgc_ny_early_short_exit_overlay_research",
        "artifact_paths": {"json": str(json_path), "markdown": str(markdown_path)},
        "variant_reports": variant_reports,
    }


def build_overlay_specs() -> tuple[ExitOverlaySpec, ...]:
    return (
        ExitOverlaySpec(
            overlay_id="fast_fail_first_bar_adverse",
            description="Exit at the first bar close if the trade closes above entry and never achieves 0.20R MFE.",
            first_bar_max_close_r=0.0,
            first_bar_min_mfe_r=0.20,
        ),
        ExitOverlaySpec(
            overlay_id="fast_fail_two_bar_no_extension",
            description="Exit at the second bar close if the trade fails to achieve 0.50R MFE within two bars.",
            second_bar_min_mfe_r=0.50,
        ),
        ExitOverlaySpec(
            overlay_id="fast_fail_hybrid",
            description="First-bar adverse close check plus the two-bar 0.35R progress check.",
            first_bar_max_close_r=0.0,
            first_bar_min_mfe_r=0.20,
            second_bar_min_mfe_r=0.35,
        ),
    )


def _load_selected_test_rows(*, meta_report: dict[str, Any]) -> list[dict[str, str]]:
    dataset_path = Path(meta_report["dataset_path"])
    rows: list[dict[str, str]] = []
    with dataset_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["split"] != "test":
                continue
            if str(row["selected"]).lower() != "true":
                continue
            rows.append(row)
    return rows


def _baseline_result(*, symbol: str, trade_date: str, source_variant: str, session: dict[str, Any]) -> OverlayTradeResult:
    return OverlayTradeResult(
        symbol=symbol,
        trade_date=trade_date,
        source_variant=source_variant,
        overlay_id="baseline",
        entry_bar_number=int(session["entry_bar_number"]),
        exit_bar_number=int(session["exit_bar_number"]),
        exit_reason=str(session["exit_reason"]),
        pnl_points=float(session["pnl_points"]),
        net_pnl_points=float(session["net_pnl_points"]),
        gross_r_multiple=float(session["gross_r_multiple"]) if session.get("gross_r_multiple") is not None else None,
        net_r_multiple=float(session["net_r_multiple"]) if session.get("net_r_multiple") is not None else None,
        mfe_points=float(session["mfe_points"]),
        mae_points=float(session["mae_points"]),
    )


def _apply_overlay(
    *,
    symbol: str,
    trade_date: str,
    source_variant: str,
    session: dict[str, Any],
    segment_bars: list[ResearchBar],
    spec: NyEarlyShortSpec,
    overlay: ExitOverlaySpec,
) -> OverlayTradeResult:
    entry_index = int(session["entry_bar_number"]) - 1
    entry_price = float(session["entry_price"])
    stop_price = float(session["stop_price"])
    risk_points = float(session["risk_points"])
    if entry_index < 0 or entry_index >= len(segment_bars):
        return OverlayTradeResult(
            symbol=symbol,
            trade_date=trade_date,
            source_variant=source_variant,
            overlay_id=overlay.overlay_id,
            entry_bar_number=int(session["entry_bar_number"]),
            exit_bar_number=int(session["exit_bar_number"]),
            exit_reason="overlay_unavailable_invalid_entry_index",
            pnl_points=float(session["pnl_points"]),
            net_pnl_points=float(session["net_pnl_points"]),
            gross_r_multiple=float(session["gross_r_multiple"]) if session.get("gross_r_multiple") is not None else None,
            net_r_multiple=float(session["net_r_multiple"]) if session.get("net_r_multiple") is not None else None,
            mfe_points=float(session["mfe_points"]),
            mae_points=float(session["mae_points"]),
        )
    ema_values = _ema([bar.close for bar in segment_bars], length=spec.exit_ema_length)

    overlay_exit = _overlay_exit_index(
        segment_bars=segment_bars,
        entry_index=entry_index,
        entry_price=entry_price,
        risk_points=risk_points,
        overlay=overlay,
    )
    if overlay_exit is None:
        exit_index, exit_price, exit_reason = _find_exit(
            segment_bars=segment_bars,
            entry_index=entry_index,
            stop_price=stop_price,
            ema_values=ema_values,
            spec=spec,
        )
    else:
        exit_index, exit_reason = overlay_exit
        exit_price = round(float(segment_bars[exit_index].close), 4)

    pnl_points = round(entry_price - exit_price, 4)
    economics = _contract_economics(symbol, spec=spec)
    execution_cost_points = round(_execution_cost_points(spec=spec, economics=economics), 4)
    net_pnl_points = round(pnl_points - execution_cost_points, 4)
    trade_window = segment_bars[entry_index : exit_index + 1]
    mfe_points = round(max(0.0, entry_price - min(bar.low for bar in trade_window)), 4)
    mae_points = round(max(0.0, max(bar.high for bar in trade_window) - entry_price), 4)
    gross_r_multiple = round(pnl_points / risk_points, 4) if risk_points > 0 else None
    net_r_multiple = round(net_pnl_points / risk_points, 4) if risk_points > 0 else None
    return OverlayTradeResult(
        symbol=symbol,
        trade_date=trade_date,
        source_variant=source_variant,
        overlay_id=overlay.overlay_id,
        entry_bar_number=entry_index + 1,
        exit_bar_number=exit_index + 1,
        exit_reason=exit_reason,
        pnl_points=pnl_points,
        net_pnl_points=net_pnl_points,
        gross_r_multiple=gross_r_multiple,
        net_r_multiple=net_r_multiple,
        mfe_points=mfe_points,
        mae_points=mae_points,
    )


def _overlay_exit_index(
    *,
    segment_bars: list[ResearchBar],
    entry_index: int,
    entry_price: float,
    risk_points: float,
    overlay: ExitOverlaySpec,
) -> tuple[int, str] | None:
    first_index = entry_index
    if overlay.first_bar_max_close_r is not None or overlay.first_bar_min_mfe_r is not None:
        first_bar = segment_bars[first_index]
        close_r = (entry_price - float(first_bar.close)) / risk_points if risk_points > 0 else 0.0
        mfe_r = (entry_price - float(first_bar.low)) / risk_points if risk_points > 0 else 0.0
        close_check = overlay.first_bar_max_close_r is None or close_r <= overlay.first_bar_max_close_r
        mfe_check = overlay.first_bar_min_mfe_r is None or mfe_r < overlay.first_bar_min_mfe_r
        if close_check and mfe_check:
            return first_index, overlay.overlay_id

    if overlay.second_bar_min_mfe_r is not None:
        second_index = min(entry_index + 1, len(segment_bars) - 1)
        low = min(bar.low for bar in segment_bars[entry_index : second_index + 1])
        mfe_r = (entry_price - float(low)) / risk_points if risk_points > 0 else 0.0
        if mfe_r < overlay.second_bar_min_mfe_r:
            return second_index, overlay.overlay_id
    return None


def _summarize_overlay_results(rows: list[OverlayTradeResult]) -> dict[str, Any]:
    winners = [row for row in rows if row.net_pnl_points > 0.0]
    losers = [row for row in rows if row.net_pnl_points <= 0.0]
    gross_profit = sum(row.net_pnl_points for row in winners)
    gross_loss = abs(sum(row.net_pnl_points for row in losers))
    by_symbol = {}
    for symbol in sorted({row.symbol for row in rows}):
        symbol_rows = [row for row in rows if row.symbol == symbol]
        s_winners = [row for row in symbol_rows if row.net_pnl_points > 0.0]
        s_losers = [row for row in symbol_rows if row.net_pnl_points <= 0.0]
        s_profit = sum(row.net_pnl_points for row in s_winners)
        s_loss = abs(sum(row.net_pnl_points for row in s_losers))
        by_symbol[symbol] = {
            "trade_count": len(symbol_rows),
            "average_net_pnl_points": round(statistics.fmean(row.net_pnl_points for row in symbol_rows), 4),
            "median_net_pnl_points": round(statistics.median(row.net_pnl_points for row in symbol_rows), 4),
            "net_profit_factor": round(s_profit / s_loss, 4) if s_loss > 0 else None,
            "win_rate": round(len(s_winners) / len(symbol_rows), 4) if symbol_rows else None,
        }
    return {
        "trade_count": len(rows),
        "average_net_pnl_points": round(statistics.fmean(row.net_pnl_points for row in rows), 4) if rows else None,
        "median_net_pnl_points": round(statistics.median(row.net_pnl_points for row in rows), 4) if rows else None,
        "net_profit_factor": round(gross_profit / gross_loss, 4) if gross_loss > 0 else None,
        "win_rate": round(len(winners) / len(rows), 4) if rows else None,
        "average_mae_points": round(statistics.fmean(row.mae_points for row in rows), 4) if rows else None,
        "average_mfe_points": round(statistics.fmean(row.mfe_points for row in rows), 4) if rows else None,
        "by_symbol": by_symbol,
    }


def _ema(values: list[float], *, length: int) -> list[float | None]:
    alpha = 2.0 / (length + 1.0)
    ema_value: float | None = None
    output: list[float | None] = [None] * len(values)
    for index, value in enumerate(values):
        if ema_value is None:
            ema_value = float(value)
        else:
            ema_value = alpha * float(value) + (1.0 - alpha) * ema_value
        output[index] = ema_value
    return output


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# GC/MGC NY Early Short Exit Overlay Research",
        "",
        f"- Generated at: `{payload['generated_at']}`",
        f"- Source JSON: `{payload['source_json']}`",
        f"- Meta JSON: `{payload['meta_json']}`",
        "",
    ]
    for variant in payload["variant_reports"]:
        lines.append(f"## {variant['source_variant']}")
        if variant.get("status"):
            lines.append("")
            lines.append(f"- Status: `{variant['status']}`")
            lines.append("")
            continue
        baseline = variant["baseline_summary"]
        lines.append("")
        lines.append(
            f"- Baseline selected set: trades `{baseline['trade_count']}`, avg net `{baseline['average_net_pnl_points']}`, PF `{baseline['net_profit_factor']}`"
        )
        for overlay_id, overlay_payload in variant["overlay_summaries"].items():
            summary = overlay_payload["summary"]
            lines.append(
                f"- `{overlay_id}`: trades `{summary['trade_count']}`, avg net `{summary['average_net_pnl_points']}`, PF `{summary['net_profit_factor']}`"
            )
        lines.append("")
    return "\n".join(lines)


def _resolve_database_path(value: str) -> Path:
    raw = str(value)
    if raw.startswith("sqlite:///"):
        return Path(raw.removeprefix("sqlite:///")).resolve()
    return Path(raw).resolve()
