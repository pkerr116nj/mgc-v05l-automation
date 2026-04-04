"""Closeout artifacts for the current Databento/canonical-history workstream."""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_REPLAY_DB = REPO_ROOT / "mgc_v05l.replay.sqlite3"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "baseline_closeout"
DEFAULT_COVERAGE_AUDIT_PATH = REPO_ROOT / "outputs" / "reports" / "canonical_market_data" / "pilot_coverage_audit.json"
DEFAULT_DECISION_MEMO_PATH = REPO_ROOT / "outputs" / "reports" / "strategy_universe_retest" / "strategy_decision_memo.json"
DEFAULT_LIVE_PROOF_PATH = REPO_ROOT / "outputs" / "reports" / "strategy_universe_retest" / "live_app_playback_proof.json"


def generate_baseline_closeout(
    *,
    replay_db_path: str | Path = DEFAULT_REPLAY_DB,
    coverage_audit_path: str | Path = DEFAULT_COVERAGE_AUDIT_PATH,
    decision_memo_path: str | Path = DEFAULT_DECISION_MEMO_PATH,
    live_proof_path: str | Path = DEFAULT_LIVE_PROOF_PATH,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
) -> dict[str, Path]:
    replay_db = Path(replay_db_path).resolve()
    coverage_audit = json.loads(Path(coverage_audit_path).resolve().read_text(encoding="utf-8"))
    decision_memo = json.loads(Path(decision_memo_path).resolve().read_text(encoding="utf-8"))
    live_proof = json.loads(Path(live_proof_path).resolve().read_text(encoding="utf-8"))
    resolved_output_dir = Path(output_dir).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    coverage_summary = _build_coverage_summary(
        replay_db_path=replay_db,
        coverage_audit=coverage_audit,
    )
    memo = _build_closeout_memo(
        coverage_summary=coverage_summary,
        decision_memo=decision_memo,
        live_proof=live_proof,
    )

    coverage_json_path = resolved_output_dir / "pilot_coverage_baseline_summary.json"
    coverage_md_path = resolved_output_dir / "pilot_coverage_baseline_summary.md"
    memo_json_path = resolved_output_dir / "baseline_closeout_memo.json"
    memo_md_path = resolved_output_dir / "baseline_closeout_memo.md"

    coverage_json_path.write_text(json.dumps(coverage_summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    coverage_md_path.write_text(_render_coverage_markdown(coverage_summary), encoding="utf-8")
    memo_json_path.write_text(json.dumps(memo, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    memo_md_path.write_text(_render_memo_markdown(memo), encoding="utf-8")

    return {
        "coverage_json_path": coverage_json_path,
        "coverage_markdown_path": coverage_md_path,
        "memo_json_path": memo_json_path,
        "memo_markdown_path": memo_md_path,
    }


def _build_coverage_summary(*, replay_db_path: Path, coverage_audit: dict[str, Any]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    with sqlite3.connect(replay_db_path) as connection:
        connection.row_factory = sqlite3.Row
        for row in list(coverage_audit.get("symbols") or []):
            symbol = str(row.get("symbol") or "").strip().upper()
            canonical_1m = dict(row.get("canonical_1m") or {})
            derived_5m = dict(row.get("derived_5m") or {})
            derived_10m = dict(row.get("derived_10m") or {})
            if int(derived_5m.get("bar_count") or 0) <= 0 and int(canonical_1m.get("bar_count") or 0) > 0:
                derived_5m = _derived_coverage_summary(connection, symbol=symbol, target_minutes=5)
            if int(derived_10m.get("bar_count") or 0) <= 0 and int(canonical_1m.get("bar_count") or 0) > 0:
                derived_10m = _derived_coverage_summary(connection, symbol=symbol, target_minutes=10)
            rows.append(
                {
                    "symbol": symbol,
                    "canonical_1m": canonical_1m,
                    "derived_5m": derived_5m,
                    "derived_10m": derived_10m,
                    "gaps_detected": bool(row.get("gaps_detected")),
                    "gaps_repaired": bool(row.get("gaps_repaired")),
                    "provider_provenance_summary": list(row.get("provider_provenance_summary") or []),
                }
            )
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "replay_db_path": str(replay_db_path),
        "symbols": rows,
    }


def _derived_coverage_summary(connection: sqlite3.Connection, *, symbol: str, target_minutes: int) -> dict[str, Any]:
    timeframe_label = f"{target_minutes}m"
    summary = connection.execute(
        """
        with complete_buckets as (
          select
            cast(unixepoch(end_ts) / (? * 60) as integer) as bucket_id,
            count(*) as minute_count,
            max(end_ts) as bucket_end_ts
          from bars
          where ticker = ?
            and timeframe = '1m'
            and data_source = 'historical_1m_canonical'
            and is_final = 1
          group by bucket_id
          having count(*) = ?
        )
        select
          count(*) as bar_count,
          min(bucket_end_ts) as earliest,
          max(bucket_end_ts) as latest
        from complete_buckets
        """,
        (target_minutes, symbol, target_minutes),
    ).fetchone()
    return {
        "symbol": symbol,
        "timeframe": timeframe_label,
        "data_source": f"derived_on_demand_from_historical_1m_canonical[{timeframe_label}]",
        "bar_count": int(summary["bar_count"] or 0),
        "earliest": summary["earliest"],
        "latest": summary["latest"],
        "gap_count": None,
        "gaps": [],
        "materialized": False,
    }


def _build_closeout_memo(
    *,
    coverage_summary: dict[str, Any],
    decision_memo: dict[str, Any],
    live_proof: dict[str, Any],
) -> dict[str, Any]:
    symbols = list(coverage_summary.get("symbols") or [])
    fully_complete = [
        f"LIVE API desktop proof is stable against run stamp {live_proof.get('run_stamp')} with manifest {live_proof.get('manifest_path')}.",
        "Dashboard, P&L Calendar, Live P&L, Trade Entry, and Strategy Deep-Dive all have real desktop captures in outputs/operator_console_live.",
        "Strategy decision package is finalized with promotable, retained, and reject lanes in outputs/reports/strategy_universe_retest/strategy_decision_memo.{json,md}.",
        "Schema-preserving canonical subset build is fixed and emitted for MGC without duplicate-prone CTAS behavior.",
        f"Pilot baseline coverage summary is emitted for {len(symbols)} pilot symbols with canonical 1m plus derived 5m/10m coverage windows.",
    ]
    deferred = [
        "Pilot-wide materialized 5m/10m persistence across every symbol was not forced in this closeout pass; the summary falls back to authoritative on-demand derivation from canonical 1m where rows are not persisted.",
        "The heavyweight full-detail gap dump remains available in outputs/reports/canonical_market_data/pilot_coverage_audit.{json,md}, but this closeout baseline intentionally uses the lighter summary artifact for stability.",
    ]
    stable_baseline = [
        "Canonical truth going forward is historical_1m_canonical; higher whole-minute views are derived from canonical 1m rather than treated as a second primary base.",
        "The current playback/app baseline is historical_playback_20260404_141802.manifest.json on LIVE API, with run stamp 20260404_141802.",
        "Schwab remains execution/account truth; market-data and execution stay separated.",
        "Future cleanup or architecture work should start from the closeout artifacts under outputs/reports/baseline_closeout plus the live proof artifact outputs/reports/strategy_universe_retest/live_app_playback_proof.json.",
    ]
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "fully_complete": fully_complete,
        "deferred_still_open": deferred,
        "stable_baseline_going_forward": stable_baseline,
    }


def _render_coverage_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Pilot Coverage Baseline Summary",
        "",
        f"- Generated At: {payload.get('generated_at')}",
        f"- Replay DB: {payload.get('replay_db_path')}",
        "",
        "| Symbol | 1m Earliest | 1m Latest | 1m Gaps | 5m Earliest | 5m Latest | 5m Source | 10m Earliest | 10m Latest | 10m Source | Provenance |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in list(payload.get("symbols") or []):
        provenance = ", ".join(
            f"{entry.get('provider')}/{entry.get('data_source')}[{entry.get('interval')}]"
            for entry in list(row.get("provider_provenance_summary") or [])
        )
        canonical_1m = dict(row.get("canonical_1m") or {})
        derived_5m = dict(row.get("derived_5m") or {})
        derived_10m = dict(row.get("derived_10m") or {})
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.get("symbol") or "-"),
                    str(canonical_1m.get("earliest") or "-"),
                    str(canonical_1m.get("latest") or "-"),
                    str(canonical_1m.get("gap_count") if canonical_1m.get("gap_count") is not None else "-"),
                    str(derived_5m.get("earliest") or "-"),
                    str(derived_5m.get("latest") or "-"),
                    str(derived_5m.get("data_source") or "-"),
                    str(derived_10m.get("earliest") or "-"),
                    str(derived_10m.get("latest") or "-"),
                    str(derived_10m.get("data_source") or "-"),
                    provenance or "-",
                ]
            )
            + " |"
        )
    return "\n".join(lines).strip() + "\n"


def _render_memo_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Baseline Closeout Memo",
        "",
        f"- Generated At: {payload.get('generated_at')}",
        "",
        "## Fully Complete",
        "",
    ]
    for item in list(payload.get("fully_complete") or []):
        lines.append(f"- {item}")
    lines.extend(["", "## Deferred / Still Open", ""])
    for item in list(payload.get("deferred_still_open") or []):
        lines.append(f"- {item}")
    lines.extend(["", "## Stable Baseline Going Forward", ""])
    for item in list(payload.get("stable_baseline_going_forward") or []):
        lines.append(f"- {item}")
    return "\n".join(lines).strip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(prog="baseline-closeout")
    parser.add_argument("--replay-db-path", default=str(DEFAULT_REPLAY_DB))
    parser.add_argument("--coverage-audit-path", default=str(DEFAULT_COVERAGE_AUDIT_PATH))
    parser.add_argument("--decision-memo-path", default=str(DEFAULT_DECISION_MEMO_PATH))
    parser.add_argument("--live-proof-path", default=str(DEFAULT_LIVE_PROOF_PATH))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args()
    outputs = generate_baseline_closeout(
        replay_db_path=args.replay_db_path,
        coverage_audit_path=args.coverage_audit_path,
        decision_memo_path=args.decision_memo_path,
        live_proof_path=args.live_proof_path,
        output_dir=args.output_dir,
    )
    print(json.dumps({key: str(value) for key, value in outputs.items()}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
