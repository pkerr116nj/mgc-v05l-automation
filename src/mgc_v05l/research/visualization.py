"""Research-only visualization helpers for persisted EMA momentum data."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Sequence

from sqlalchemy import and_, select

from ..persistence.repositories import RepositorySet
from ..persistence.tables import bars_table, derived_features_table, signal_evaluations_table


@dataclass(frozen=True)
class EMAMomentumVisualizationRequest:
    experiment_run_id: int
    ticker: str | None = None
    timeframe: str | None = None
    start_timestamp: datetime | None = None
    end_timestamp: datetime | None = None
    limit: int | None = None


@dataclass(frozen=True)
class EMAMomentumVisualizationRow:
    bar_id: str
    ticker: str
    timeframe: str
    timestamp: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    vwap: Decimal | None
    smoothed_close: Decimal | None
    momentum_norm: Decimal | None
    momentum_acceleration: Decimal | None
    signed_impulse: Decimal | None
    smoothed_signed_impulse: Decimal | None
    trigger_long_math: bool
    trigger_short_math: bool
    compression_long: bool
    compression_short: bool
    reclaim_long: bool
    failure_short: bool
    separation_long: bool
    separation_short: bool
    structure_long_candidate: bool
    structure_short_candidate: bool
    warmup_complete: bool


def load_ema_momentum_visualization_rows(
    repositories: RepositorySet,
    request: EMAMomentumVisualizationRequest,
) -> list[EMAMomentumVisualizationRow]:
    """Load filtered research rows for visualization from persisted SQLite data."""
    statement = (
        select(
            bars_table.c.bar_id,
            bars_table.c.ticker,
            bars_table.c.timeframe,
            bars_table.c.timestamp,
            bars_table.c.open,
            bars_table.c.high,
            bars_table.c.low,
            bars_table.c.close,
            derived_features_table.c.vwap,
            derived_features_table.c.smoothed_close,
            derived_features_table.c.momentum_norm,
            derived_features_table.c.momentum_acceleration,
            derived_features_table.c.signed_impulse,
            derived_features_table.c.smoothed_signed_impulse,
            signal_evaluations_table.c.trigger_long_math,
            signal_evaluations_table.c.trigger_short_math,
            signal_evaluations_table.c.compression_long,
            signal_evaluations_table.c.compression_short,
            signal_evaluations_table.c.reclaim_long,
            signal_evaluations_table.c.failure_short,
            signal_evaluations_table.c.separation_long,
            signal_evaluations_table.c.separation_short,
            signal_evaluations_table.c.structure_long_candidate,
            signal_evaluations_table.c.structure_short_candidate,
            signal_evaluations_table.c.warmup_complete,
        )
        .select_from(
            bars_table.join(
                signal_evaluations_table,
                bars_table.c.bar_id == signal_evaluations_table.c.bar_id,
            ).join(
                derived_features_table,
                and_(
                    bars_table.c.bar_id == derived_features_table.c.bar_id,
                    signal_evaluations_table.c.experiment_run_id == derived_features_table.c.experiment_run_id,
                ),
            )
        )
        .where(signal_evaluations_table.c.experiment_run_id == request.experiment_run_id)
        .order_by(bars_table.c.timestamp.asc(), bars_table.c.bar_id.asc())
    )
    if request.ticker is not None:
        statement = statement.where(bars_table.c.ticker == request.ticker)
    if request.timeframe is not None:
        statement = statement.where(bars_table.c.timeframe == request.timeframe)
    if request.start_timestamp is not None:
        statement = statement.where(bars_table.c.timestamp >= request.start_timestamp.isoformat())
    if request.end_timestamp is not None:
        statement = statement.where(bars_table.c.timestamp <= request.end_timestamp.isoformat())
    if request.limit is not None:
        statement = statement.limit(request.limit)

    with repositories.engine.begin() as connection:
        rows = connection.execute(statement).mappings().all()
    return [_decode_visualization_row(dict(row)) for row in rows]


def write_ema_momentum_visualization_html(
    rows: Sequence[EMAMomentumVisualizationRow],
    output_path: str | Path,
    *,
    title: str | None = None,
) -> Path:
    """Write a self-contained research-only HTML chart artifact."""
    path = Path(output_path)
    payload = [_row_to_json_ready(row) for row in rows]
    chart_title = title or "EMA Momentum Research Visualization"
    html = _build_html(chart_title, payload)
    path.write_text(html, encoding="utf-8")
    return path


def _decode_visualization_row(row: dict[str, Any]) -> EMAMomentumVisualizationRow:
    return EMAMomentumVisualizationRow(
        bar_id=row["bar_id"],
        ticker=row["ticker"],
        timeframe=row["timeframe"],
        timestamp=row["timestamp"],
        open=_to_decimal(row["open"]) or Decimal("0"),
        high=_to_decimal(row["high"]) or Decimal("0"),
        low=_to_decimal(row["low"]) or Decimal("0"),
        close=_to_decimal(row["close"]) or Decimal("0"),
        vwap=_to_decimal(row["vwap"]),
        smoothed_close=_to_decimal(row["smoothed_close"]),
        momentum_norm=_to_decimal(row["momentum_norm"]),
        momentum_acceleration=_to_decimal(row["momentum_acceleration"]),
        signed_impulse=_to_decimal(row["signed_impulse"]),
        smoothed_signed_impulse=_to_decimal(row["smoothed_signed_impulse"]),
        trigger_long_math=bool(row["trigger_long_math"]),
        trigger_short_math=bool(row["trigger_short_math"]),
        compression_long=bool(row["compression_long"]),
        compression_short=bool(row["compression_short"]),
        reclaim_long=bool(row["reclaim_long"]),
        failure_short=bool(row["failure_short"]),
        separation_long=bool(row["separation_long"]),
        separation_short=bool(row["separation_short"]),
        structure_long_candidate=bool(row["structure_long_candidate"]),
        structure_short_candidate=bool(row["structure_short_candidate"]),
        warmup_complete=bool(row["warmup_complete"]),
    )


def _row_to_json_ready(row: EMAMomentumVisualizationRow) -> dict[str, Any]:
    return {
        "bar_id": row.bar_id,
        "ticker": row.ticker,
        "timeframe": row.timeframe,
        "timestamp": row.timestamp,
        "open": _to_float(row.open),
        "high": _to_float(row.high),
        "low": _to_float(row.low),
        "close": _to_float(row.close),
        "vwap": _to_float(row.vwap),
        "smoothed_close": _to_float(row.smoothed_close),
        "momentum_norm": _to_float(row.momentum_norm),
        "momentum_acceleration": _to_float(row.momentum_acceleration),
        "signed_impulse": _to_float(row.signed_impulse),
        "smoothed_signed_impulse": _to_float(row.smoothed_signed_impulse),
        "trigger_long_math": row.trigger_long_math,
        "trigger_short_math": row.trigger_short_math,
        "compression_long": row.compression_long,
        "compression_short": row.compression_short,
        "reclaim_long": row.reclaim_long,
        "failure_short": row.failure_short,
        "separation_long": row.separation_long,
        "separation_short": row.separation_short,
        "structure_long_candidate": row.structure_long_candidate,
        "structure_short_candidate": row.structure_short_candidate,
        "warmup_complete": row.warmup_complete,
    }


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))


def _to_float(value: Decimal | None) -> float | None:
    if value is None:
        return None
    return float(value)


def _build_html(title: str, payload: list[dict[str, Any]]) -> str:
    data_json = json.dumps(payload)
    title_json = json.dumps(title)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title}</title>
  <style>
    :root {{
      --bg: #f6f3ea;
      --panel: #fffdf8;
      --ink: #1e1d1a;
      --muted: #70685b;
      --grid: #ddd2be;
      --up: #1a7f53;
      --down: #b5422c;
      --vwap: #355c7d;
      --smooth: #c06c2b;
      --accent: #7c3aed;
    }}
    body {{
      margin: 0;
      font-family: Georgia, "Iowan Old Style", "Palatino Linotype", serif;
      color: var(--ink);
      background: linear-gradient(180deg, #efe8da 0%, var(--bg) 100%);
    }}
    .wrap {{
      max-width: 1400px;
      margin: 0 auto;
      padding: 24px;
    }}
    h1 {{
      margin: 0 0 10px;
      font-size: 28px;
    }}
    .meta {{
      color: var(--muted);
      margin-bottom: 14px;
    }}
    .controls {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px 16px;
      padding: 14px 16px;
      background: rgba(255,255,255,0.7);
      border: 1px solid var(--grid);
      border-radius: 14px;
      margin-bottom: 18px;
    }}
    .controls label {{
      font-size: 14px;
      display: inline-flex;
      gap: 6px;
      align-items: center;
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--grid);
      border-radius: 18px;
      padding: 12px;
      margin-bottom: 14px;
      box-shadow: 0 8px 24px rgba(60,40,10,0.06);
    }}
    svg {{
      width: 100%;
      display: block;
    }}
    .legend {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px 14px;
      color: var(--muted);
      font-size: 13px;
      margin-bottom: 10px;
    }}
    .legend span::before {{
      content: "";
      display: inline-block;
      width: 10px;
      height: 10px;
      margin-right: 6px;
      border-radius: 999px;
      vertical-align: middle;
    }}
    .legend .vwap::before {{ background: var(--vwap); }}
    .legend .smooth::before {{ background: var(--smooth); }}
    .legend .long::before {{ background: var(--up); }}
    .legend .short::before {{ background: var(--down); }}
    .legend .struct::before {{ background: var(--accent); }}
    .footer-note {{
      color: var(--muted);
      font-size: 13px;
      margin-top: 14px;
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>{title}</h1>
    <div class="meta" id="meta"></div>
    <div class="controls">
      <label><input type="checkbox" id="toggle-vwap" checked> VWAP</label>
      <label><input type="checkbox" id="toggle-smooth" checked> Smoothed close</label>
      <label><input type="checkbox" id="toggle-math" checked> Math triggers</label>
      <label><input type="checkbox" id="toggle-structure" checked> Structure candidates</label>
      <label><input type="checkbox" id="toggle-detail"> Structure detail markers</label>
      <label><input type="checkbox" id="toggle-warmup"> Highlight warmup-incomplete</label>
    </div>
    <div class="panel">
      <div class="legend">
        <span class="vwap">VWAP</span>
        <span class="smooth">Smoothed close</span>
        <span class="long">Long labels</span>
        <span class="short">Short labels</span>
        <span class="struct">Structure candidates</span>
      </div>
      <svg id="price-chart" viewBox="0 0 1200 440" preserveAspectRatio="none"></svg>
    </div>
    <div class="panel">
      <svg id="feature-chart-1" viewBox="0 0 1200 220" preserveAspectRatio="none"></svg>
    </div>
    <div class="panel">
      <svg id="feature-chart-2" viewBox="0 0 1200 220" preserveAspectRatio="none"></svg>
    </div>
    <div class="footer-note">
      Research-only historical visualization for persisted EMA momentum labels and features. This is not a production trading UI.
    </div>
  </div>
  <script>
    const TITLE = {title_json};
    const data = {data_json};

    function linePath(points) {{
      return points.map((p, i) => `${{i === 0 ? 'M' : 'L'}} ${{p[0].toFixed(2)}} ${{p[1].toFixed(2)}}`).join(' ');
    }}

    function scaleSeries(values, top, bottom) {{
      const clean = values.filter(v => v !== null && !Number.isNaN(v));
      const min = clean.length ? Math.min(...clean) : 0;
      const max = clean.length ? Math.max(...clean) : 1;
      const pad = min === max ? Math.max(1, Math.abs(min) * 0.1) : (max - min) * 0.08;
      const low = min - pad;
      const high = max + pad;
      return value => {{
        if (value === null || Number.isNaN(value)) return null;
        return bottom - ((value - low) / (high - low || 1)) * (bottom - top);
      }};
    }}

    function hsl(color) {{ return color; }}

    function buildPriceChart() {{
      const svg = document.getElementById('price-chart');
      const width = 1200, height = 440;
      const pad = {{ left: 56, right: 20, top: 20, bottom: 34 }};
      const plotW = width - pad.left - pad.right;
      const plotH = height - pad.top - pad.bottom;
      const xs = data.map((_, i) => pad.left + (plotW * i) / Math.max(1, data.length - 1));
      const ys = scaleSeries(
        data.flatMap(d => [d.high, d.low, d.vwap, d.smoothed_close].filter(v => v !== null)),
        pad.top,
        pad.top + plotH
      );
      let html = '';
      for (let i = 0; i < 5; i++) {{
        const y = pad.top + (plotH * i) / 4;
        html += `<line x1="${{pad.left}}" y1="${{y}}" x2="${{width - pad.right}}" y2="${{y}}" stroke="#ddd2be" stroke-width="1"/>`;
      }}
      data.forEach((row, index) => {{
        const x = xs[index];
        const bodyW = Math.max(4, plotW / Math.max(12, data.length * 1.7));
        const o = ys(row.open), h = ys(row.high), l = ys(row.low), c = ys(row.close);
        const up = row.close >= row.open;
        const color = up ? '#1a7f53' : '#b5422c';
        const y = Math.min(o, c);
        const bodyH = Math.max(1.5, Math.abs(c - o));
        const warmupClass = (!row.warmup_complete && document.getElementById('toggle-warmup').checked) ? '0.45' : '1';
        html += `<line x1="${{x}}" y1="${{h}}" x2="${{x}}" y2="${{l}}" stroke="${{color}}" stroke-width="1.4" opacity="${{warmupClass}}"/>`;
        html += `<rect x="${{x - bodyW/2}}" y="${{y}}" width="${{bodyW}}" height="${{bodyH}}" fill="${{up ? '#d8f0e1' : '#f3d7d2'}}" stroke="${{color}}" opacity="${{warmupClass}}"/>`;
      }});
      const vwapPts = data.map((row, i) => row.vwap === null ? null : [xs[i], ys(row.vwap)]).filter(Boolean);
      const smoothPts = data.map((row, i) => row.smoothed_close === null ? null : [xs[i], ys(row.smoothed_close)]).filter(Boolean);
      if (document.getElementById('toggle-vwap').checked && vwapPts.length > 1) {{
        html += `<path d="${{linePath(vwapPts)}}" fill="none" stroke="#355c7d" stroke-width="2"/>`;
      }}
      if (document.getElementById('toggle-smooth').checked && smoothPts.length > 1) {{
        html += `<path d="${{linePath(smoothPts)}}" fill="none" stroke="#c06c2b" stroke-width="2"/>`;
      }}
      data.forEach((row, index) => {{
        const x = xs[index];
        const highY = ys(row.high);
        const lowY = ys(row.low);
        if (document.getElementById('toggle-math').checked && row.trigger_long_math) {{
          html += `<circle cx="${{x}}" cy="${{lowY + 12}}" r="5" fill="#1a7f53"/>`;
        }}
        if (document.getElementById('toggle-math').checked && row.trigger_short_math) {{
          html += `<circle cx="${{x}}" cy="${{highY - 12}}" r="5" fill="#b5422c"/>`;
        }}
        if (document.getElementById('toggle-structure').checked && row.structure_long_candidate) {{
          html += `<rect x="${{x - 4}}" y="${{lowY + 22}}" width="8" height="8" fill="#7c3aed"/>`;
        }}
        if (document.getElementById('toggle-structure').checked && row.structure_short_candidate) {{
          html += `<rect x="${{x - 4}}" y="${{highY - 30}}" width="8" height="8" fill="#7c3aed"/>`;
        }}
        if (document.getElementById('toggle-detail').checked) {{
          if (row.compression_long) html += `<text x="${{x + 5}}" y="${{lowY + 36}}" font-size="11" fill="#1a7f53">CL</text>`;
          if (row.reclaim_long) html += `<text x="${{x + 5}}" y="${{lowY + 48}}" font-size="11" fill="#1a7f53">RL</text>`;
          if (row.separation_long) html += `<text x="${{x + 5}}" y="${{lowY + 60}}" font-size="11" fill="#1a7f53">SL</text>`;
          if (row.compression_short) html += `<text x="${{x + 5}}" y="${{highY - 48}}" font-size="11" fill="#b5422c">CS</text>`;
          if (row.failure_short) html += `<text x="${{x + 5}}" y="${{highY - 36}}" font-size="11" fill="#b5422c">FS</text>`;
          if (row.separation_short) html += `<text x="${{x + 5}}" y="${{highY - 24}}" font-size="11" fill="#b5422c">SS</text>`;
        }}
      }});
      svg.innerHTML = html;
    }}

    function buildFeatureChart(svgId, seriesDefs, title) {{
      const svg = document.getElementById(svgId);
      const width = 1200, height = 220;
      const pad = {{ left: 56, right: 20, top: 18, bottom: 28 }};
      const plotW = width - pad.left - pad.right;
      const plotH = height - pad.top - pad.bottom;
      const xs = data.map((_, i) => pad.left + (plotW * i) / Math.max(1, data.length - 1));
      const ys = scaleSeries(
        data.flatMap(row => seriesDefs.map(def => row[def.key]).filter(v => v !== null)),
        pad.top,
        pad.top + plotH
      );
      let html = `<text x="${{pad.left}}" y="14" font-size="13" fill="#70685b">${{title}}</text>`;
      const zeroY = ys(0);
      if (zeroY !== null) {{
        html += `<line x1="${{pad.left}}" y1="${{zeroY}}" x2="${{width - pad.right}}" y2="${{zeroY}}" stroke="#cfc3ae" stroke-width="1.2"/>`;
      }}
      for (let i = 0; i < 4; i++) {{
        const y = pad.top + (plotH * i) / 3;
        html += `<line x1="${{pad.left}}" y1="${{y}}" x2="${{width - pad.right}}" y2="${{y}}" stroke="#ede4d6" stroke-width="1"/>`;
      }}
      seriesDefs.forEach(def => {{
        const pts = data.map((row, i) => row[def.key] === null ? null : [xs[i], ys(row[def.key])]).filter(Boolean);
        if (pts.length > 1) {{
          html += `<path d="${{linePath(pts)}}" fill="none" stroke="${{def.color}}" stroke-width="2"/>`;
        }}
      }});
      svg.innerHTML = html;
    }}

    function render() {{
      const first = data[0];
      const last = data[data.length - 1];
      const meta = data.length
        ? `${{first.ticker}} | ${{first.timeframe}} | bars: ${{data.length}} | ${{first.timestamp}} -> ${{last.timestamp}}`
        : 'No rows matched the visualization filter.';
      document.getElementById('meta').textContent = meta;
      buildPriceChart();
      buildFeatureChart('feature-chart-1', [
        {{ key: 'momentum_norm', color: '#355c7d' }},
        {{ key: 'momentum_acceleration', color: '#7c3aed' }},
      ], 'Momentum Norm + Momentum Acceleration');
      buildFeatureChart('feature-chart-2', [
        {{ key: 'signed_impulse', color: '#b5422c' }},
        {{ key: 'smoothed_signed_impulse', color: '#c06c2b' }},
      ], 'Signed Impulse + Smoothed Signed Impulse');
    }}

    ['toggle-vwap','toggle-smooth','toggle-math','toggle-structure','toggle-detail','toggle-warmup']
      .forEach(id => document.getElementById(id).addEventListener('change', render));
    render();
  </script>
</body>
</html>"""
