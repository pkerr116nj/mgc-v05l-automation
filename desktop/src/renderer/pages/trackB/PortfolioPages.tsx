import { useMemo, useState, type CSSProperties, type ReactNode } from "react";

import {
  asArray,
  asRecord,
  formatRelativeAge,
  formatShortNumber,
  formatTimestamp,
  formatValue,
  sentenceCase,
} from "../../lib/format";
import type { DesktopState, JsonRecord } from "../../types";

type Tone = "good" | "warn" | "danger" | "muted";

function nodeTitleValue(value: ReactNode): string | undefined {
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  return undefined;
}

function numericOrNull(value: unknown): number | null {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const numeric = Number(value);
  return Number.isNaN(numeric) ? null : numeric;
}

function formatCompactMetric(value: unknown, digits = 2): string {
  const numeric = numericOrNull(value);
  if (numeric === null) {
    return "—";
  }
  return new Intl.NumberFormat(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits: digits,
  }).format(numeric);
}

function formatCompactPnL(value: unknown): string {
  const numeric = numericOrNull(value);
  if (numeric === null) {
    return "—";
  }
  return `${numeric > 0 ? "+" : ""}${formatCompactMetric(numeric, 2)}`;
}

function pnlTone(value: unknown): Tone {
  const numeric = numericOrNull(value);
  if (numeric === null) {
    return "muted";
  }
  if (numeric > 0) {
    return "good";
  }
  if (numeric < 0) {
    return "danger";
  }
  return "muted";
}

function renderPnlValue(value: unknown): ReactNode {
  const numeric = numericOrNull(value);
  if (numeric === null) {
    return "—";
  }
  const className = numeric > 0 ? "pnl-positive" : numeric < 0 ? "pnl-negative" : "pnl-neutral";
  return <span className={`pnl-value ${className}`}>{formatCompactPnL(numeric)}</span>;
}

function Section(props: { title: string; subtitle?: string; children: ReactNode; className?: string; headerClassName?: string }) {
  return (
    <section className={`section-card ${props.className ?? ""}`.trim()} data-section-title={props.title}>
      <div className={`section-header ${props.headerClassName ?? ""}`.trim()}>
        <div>
          <div className="section-title">{props.title}</div>
          {props.subtitle ? <div className="section-subtitle">{props.subtitle}</div> : null}
        </div>
      </div>
      {props.children}
    </section>
  );
}

function MetricCard(props: { label: string; value: ReactNode; tone?: Tone }) {
  return (
    <div className={`metric-card ${props.tone ?? "muted"}`}>
      <div className="metric-label">{props.label}</div>
      <div className="metric-value" title={nodeTitleValue(props.value)}>
        {props.value}
      </div>
    </div>
  );
}

function Badge(props: { label: string; tone?: Tone }) {
  return <span className={`badge ${props.tone ?? "muted"}`}>{sentenceCase(props.label).toUpperCase()}</span>;
}

function DataTable(props: {
  columns: Array<{
    key: string;
    label: string;
    render?: (row: JsonRecord) => ReactNode;
    className?: string;
    sortable?: boolean;
    sortKey?: string;
  }>;
  rows: JsonRecord[];
  emptyLabel: string;
  onRowClick?: (row: JsonRecord) => void;
  rowKey?: (row: JsonRecord, index: number) => string;
  selectedRowKey?: string;
  tableClassName?: string;
  activeSortKey?: string | null;
  activeSortDirection?: "asc" | "desc" | null;
  onSortChange?: (sortKey: string) => void;
}) {
  const rows = useMemo(() => props.rows ?? [], [props.rows]);
  if (!rows.length) {
    return <div className="placeholder-note">{props.emptyLabel}</div>;
  }
  return (
    <div className="table-shell">
      <table className={`data-table ${props.tableClassName ?? ""}`.trim()}>
        <thead>
          <tr>
            {props.columns.map((column) => {
              const resolvedSortKey = column.sortKey ?? column.key;
              const active = column.sortable && props.activeSortKey === resolvedSortKey;
              const indicator = active ? (props.activeSortDirection === "asc" ? "▲" : "▼") : column.sortable ? "↕" : null;
              return (
                <th key={column.key} className={column.className}>
                  {column.sortable && props.onSortChange ? (
                    <button
                      type="button"
                      className={`table-sort-button ${active ? "is-active" : ""}`.trim()}
                      onClick={() => props.onSortChange?.(resolvedSortKey)}
                    >
                      <span>{column.label}</span>
                      {indicator ? <span className="table-sort-indicator">{indicator}</span> : null}
                    </button>
                  ) : (
                    column.label
                  )}
                </th>
              );
            })}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, index) => {
            const resolvedRowKey = props.rowKey ? props.rowKey(row, index) : String(row.id ?? row.lane_id ?? row.trade_id ?? row.symbol ?? index);
            return (
              <tr
                key={resolvedRowKey}
                className={`${props.onRowClick ? "is-clickable" : ""} ${props.selectedRowKey && resolvedRowKey === props.selectedRowKey ? "is-selected" : ""}`.trim()}
                onClick={props.onRowClick ? () => props.onRowClick?.(row) : undefined}
              >
                {props.columns.map((column) => (
                  <td key={column.key} className={column.className}>{column.render ? column.render(row) : formatValue(row[column.key])}</td>
                ))}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

export function TrackBPortfolioPage(props: { trackBPortfolio: DesktopState["trackBPortfolio"] | null }) {
  const status = props.trackBPortfolio;
  const portfolio = asRecord(status?.portfolio);
  const summary = asRecord(portfolio.portfolio_summary);
  const reconciliation = asRecord(portfolio.broker_reconciliation);
  const freshness = asRecord(portfolio.runtime_service_freshness);
  const positions = asArray<JsonRecord>(portfolio.open_positions);
  const strategyRows = asArray<JsonRecord>(portfolio.strategy_lane_pnl_summary);
  const alerts = asArray<JsonRecord>(portfolio.alerts);
  const actionRows = asArray<JsonRecord>(portfolio.action_queue);
  const sourcePolicy = asRecord(portfolio.source_policy);
  const available = status?.portfolioAvailable === true && Object.keys(portfolio).length > 0;
  const statusToneValue = portfolio.status === "OK" ? "good" : portfolio.status === "RED" ? "danger" : "warn";
  const brokerFreshnessRows = asArray<JsonRecord>(freshness.broker_truth);
  const serviceFreshnessRows = asArray<JsonRecord>(freshness.services);

  return (
    <>
      <Section title="Track B Portfolio" subtitle="Broker-authoritative PAPER positions, P&L, reconciliation, and operator alerts">
        {!available ? (
          <div className="status-banner warn">
            <div className="status-banner-main">
              <div className="status-banner-title">Portfolio Artifact Unavailable</div>
              <div className="status-banner-body">{status?.missingReason ?? "No Track B portfolio state artifact is loaded."}</div>
              <div className="status-banner-body secondary">Path: {formatValue(status?.portfolioStatePath)}</div>
            </div>
          </div>
        ) : null}
        {available ? (
          <>
            <div className={`status-banner ${statusToneValue}`}>
              <div className="status-banner-main">
                <div className="status-banner-title">TRACK B PORTFOLIO {formatValue(portfolio.status)}</div>
                <div className="status-banner-body">
                  Broker truth is authoritative for position existence. Lifecycle ownership is shown only when reconciled.
                </div>
                <div className="status-banner-body secondary">
                  Generated {formatTimestamp(portfolio.generated_at)} | Broker reconciliation {formatValue(reconciliation.status)}
                </div>
              </div>
            </div>
            <div className="badge-row">
              <Badge label={`Mode ${formatValue(portfolio.mode)}`} tone="muted" />
              <Badge label={`Live Money ${formatValue(portfolio.live_money_eligible)}`} tone={portfolio.live_money_eligible === true ? "danger" : "good"} />
              <Badge label={`Broker Positions ${formatValue(reconciliation.broker_position_count)}`} tone="muted" />
              <Badge label={`Lifecycle Positions ${formatValue(reconciliation.lifecycle_position_count)}`} tone={reconciliation.status === "MATCHED" ? "good" : "warn"} />
              <Badge label={`Research Runtime Truth ${formatValue(sourcePolicy.research_artifacts_used_as_runtime_truth)}`} tone={sourcePolicy.research_artifacts_used_as_runtime_truth === true ? "danger" : "good"} />
            </div>
            <div className="metric-grid">
              <MetricCard label="Open Positions" value={formatShortNumber(summary.open_position_count)} tone={Number(summary.open_position_count ?? 0) > 0 ? "warn" : "muted"} />
              <MetricCard label="Long Contracts" value={formatValue(summary.long_contract_count)} />
              <MetricCard label="Short Contracts" value={formatValue(summary.short_contract_count)} />
              <MetricCard label="Realized P/L" value={renderPnlValue(summary.realized_pnl)} tone={pnlTone(summary.realized_pnl)} />
              <MetricCard label="Unrealized P/L" value={renderPnlValue(summary.unrealized_pnl)} tone={pnlTone(summary.unrealized_pnl)} />
              <MetricCard label="Estimated Total" value={renderPnlValue(summary.estimated_total_pnl)} tone={pnlTone(summary.estimated_total_pnl)} />
              <MetricCard label="Open Orders" value={formatShortNumber(summary.open_order_count)} tone={Number(summary.open_order_count ?? 0) > 0 ? "warn" : "good"} />
              <MetricCard label="Review Required" value={formatShortNumber(summary.review_required_count)} tone={Number(summary.review_required_count ?? 0) > 0 ? "danger" : "good"} />
            </div>
          </>
        ) : null}
      </Section>

      {available ? (
        <>
          <Section title="Track B Open Positions" subtitle="Broker positions with lifecycle attribution, marks, P&L, and source evidence">
            <DataTable
              rows={positions}
              emptyLabel="No Track B broker positions are currently open."
              rowKey={(row, index) => `${row.symbol ?? "symbol"}-${row.localSymbol ?? "local"}-${index}`}
              columns={[
                { key: "symbol", label: "Symbol" },
                { key: "localSymbol", label: "Local" },
                { key: "expiry", label: "Expiry" },
                { key: "side", label: "Side", render: (row) => <Badge label={formatValue(row.side)} tone={row.side === "LONG" ? "good" : row.side === "SHORT" ? "danger" : "muted"} /> },
                { key: "qty", label: "Qty" },
                { key: "strategy_id", label: "Strategy" },
                { key: "lane_id", label: "Lane" },
                { key: "entry_source", label: "Entry Source", render: (row) => <Badge label={formatValue(row.entry_source)} tone={row.entry_source === "SUPERVISED_ADOPTION" ? "warn" : row.entry_source === "UNKNOWN" ? "danger" : "good"} /> },
                { key: "entry_time", label: "Entry Time", render: (row) => formatTimestamp(row.entry_time) },
                { key: "lifecycle_entry_price", label: "Lifecycle Entry" },
                { key: "broker_average_price", label: "Broker Avg" },
                { key: "last_price", label: "Last" },
                { key: "unrealized_pnl_dollars", label: "Unrealized", render: (row) => renderPnlValue(row.unrealized_pnl_dollars) },
                { key: "unrealized_pnl_points", label: "Pts" },
                { key: "bars_held", label: "Bars" },
                { key: "reconciliation_status", label: "Recon", render: (row) => <Badge label={formatValue(row.reconciliation_status)} tone={row.reconciliation_status === "MATCHED" ? "good" : "danger"} /> },
                { key: "review_required", label: "Review", render: (row) => <Badge label={formatValue(row.review_required)} tone={row.review_required === true ? "danger" : "good"} /> },
              ]}
            />
          </Section>

          <Section title="Track B Alerts" subtitle="Action queue for mismatches, stale inputs, adoption, open orders, and review markers">
            <DataTable
              rows={actionRows.length ? actionRows : alerts}
              emptyLabel="No Track B portfolio alerts are active."
              columns={[
                { key: "severity", label: "Severity", render: (row) => <Badge label={formatValue(row.severity)} tone={alertTone(row.severity)} /> },
                { key: "code", label: "Code" },
                { key: "message", label: "Message" },
                { key: "position_key", label: "Position" },
                { key: "status", label: "Status" },
              ]}
            />
          </Section>

          <Section title="Strategy / Lane P&L" subtitle="Attribution summary built from reconciled portfolio state and Track B lifecycle ledger">
            <DataTable
              rows={strategyRows}
              emptyLabel="No strategy/lane P&L rows are available."
              columns={[
                { key: "strategy_id", label: "Strategy" },
                { key: "lane_id", label: "Lane" },
                { key: "realized_pnl", label: "Realized", render: (row) => renderPnlValue(row.realized_pnl) },
                { key: "unrealized_pnl", label: "Unrealized", render: (row) => renderPnlValue(row.unrealized_pnl) },
                { key: "estimated_total_pnl", label: "Estimated", render: (row) => renderPnlValue(row.estimated_total_pnl) },
                { key: "open_position_count", label: "Open" },
                { key: "trade_count", label: "Trades" },
                { key: "review_required_count", label: "Review" },
              ]}
            />
          </Section>

          <Section title="Runtime Freshness" subtitle="Read-only freshness checks; stale inputs degrade the view instead of inferring missing truth">
            <h3 className="subsection-title">Broker Truth</h3>
            <DataTable
              rows={brokerFreshnessRows}
              emptyLabel="No broker truth freshness rows are available."
              columns={[
                { key: "name", label: "Artifact" },
                { key: "status", label: "Status", render: (row) => <Badge label={formatValue(row.status)} tone={row.status === "FRESH" ? "good" : "danger"} /> },
                { key: "generated_at", label: "Generated", render: (row) => formatTimestamp(row.generated_at) },
                { key: "age_seconds", label: "Age", render: (row) => formatRelativeAge(row.generated_at) },
                { key: "path", label: "Path" },
              ]}
            />
            <h3 className="subsection-title">Services</h3>
            <DataTable
              rows={serviceFreshnessRows}
              emptyLabel="No runtime service freshness rows are available."
              columns={[
                { key: "name", label: "Service" },
                { key: "status", label: "Status", render: (row) => <Badge label={formatValue(row.status)} tone={row.status === "FRESH" ? "good" : row.status === "MISSING" ? "muted" : "warn"} /> },
                { key: "classification", label: "Classification" },
                { key: "generated_at", label: "Generated", render: (row) => formatTimestamp(row.generated_at) },
                { key: "path", label: "Path" },
              ]}
            />
          </Section>
        </>
      ) : null}
    </>
  );
}

export function TrackBPnlCalendarPage(props: { trackBPortfolio: DesktopState["trackBPortfolio"] | null }) {
  const status = props.trackBPortfolio;
  const calendar = asRecord(status?.calendar);
  const tiles = asArray<JsonRecord>(calendar.daily_tiles);
  const drilldowns = asRecord(calendar.drilldowns);
  const todayKey = new Date().toISOString().slice(0, 10);
  const initialDay = tiles.find((tile) => tile.date === todayKey)?.date ?? tiles.find((tile) => Number(tile.trade_count ?? 0) > 0)?.date ?? String(tiles[0]?.date ?? todayKey);
  const [pnlMode, setPnlMode] = useState<"realized_only" | "realized_plus_unrealized" | "closed_trade_only">("realized_plus_unrealized");
  const [selectedDay, setSelectedDay] = useState<string>(initialDay);
  const selectedTile = asRecord(tiles.find((tile) => tile.date === selectedDay));
  const selectedDrilldown = asRecord(drilldowns[selectedDay]);
  const available = status?.calendarAvailable === true && tiles.length > 0;
  const maxAbsPnl = Math.max(
    1,
    ...tiles.map((tile) => Math.abs(Number(asRecord(tile.pnl_mode_values)[pnlMode] ?? tile.daily_estimated_total_pnl ?? 0) || 0)),
  );

  return (
    <>
      <Section title="Track B P&L Calendar" subtitle="Monthly heatmap with P&L fill and separate system-quality markers">
        {!available ? (
          <div className="status-banner warn">
            <div className="status-banner-main">
              <div className="status-banner-title">Calendar Artifact Unavailable</div>
              <div className="status-banner-body">{status?.missingReason ?? "No Track B P&L calendar artifact is loaded."}</div>
              <div className="status-banner-body secondary">Path: {formatValue(status?.pnlCalendarPath)}</div>
            </div>
          </div>
        ) : null}
        {available ? (
          <>
            <div className="calendar-toolbar">
              <div className="calendar-mode-row">
                {(["realized_plus_unrealized", "realized_only", "closed_trade_only"] as const).map((mode) => (
                  <button key={mode} className={`calendar-pill ${pnlMode === mode ? "active" : ""}`} onClick={() => setPnlMode(mode)}>
                    {mode === "realized_plus_unrealized" ? "Realized + Unrealized" : mode === "realized_only" ? "Realized Only" : "Closed Trade Only"}
                  </button>
                ))}
              </div>
              <div className="calendar-period-label">{formatValue(calendar.month)} | {formatTimestamp(calendar.generated_at)}</div>
            </div>
            <div className="calendar-alert-strip">
              <div className="calendar-alert-card good">
                <div className="calendar-alert-label">Fill Color</div>
                <div className="calendar-alert-note">Tile fill follows the selected P&L mode only.</div>
              </div>
              <div className="calendar-alert-card warn">
                <div className="calendar-alert-label">Border / Markers</div>
                <div className="calendar-alert-note">Lifecycle, broker, runtime, and data-quality issues stay visible on profitable days.</div>
              </div>
            </div>
            <div className="calendar-board-shell" data-calendar-surface="track-b-monthly">
              <div className="calendar-grid track-b-calendar-grid">
                {tiles.map((tile) => {
                  const pnlValue = Number(asRecord(tile.pnl_mode_values)[pnlMode] ?? tile.daily_estimated_total_pnl ?? 0) || 0;
                  const intensity = Math.min(Math.abs(pnlValue) / maxAbsPnl, 1);
                  const visuals = asRecord(tile.tile_visuals);
                  const borderColor = visuals.border_quality === "red" ? "rgba(215, 107, 100, 0.95)" : visuals.border_quality === "yellow" ? "rgba(214, 168, 61, 0.95)" : "rgba(124, 138, 160, 0.35)";
                  const background = pnlValue > 0
                    ? `rgba(64, 160, 96, ${0.18 + intensity * 0.38})`
                    : pnlValue < 0
                      ? `rgba(180, 74, 72, ${0.18 + intensity * 0.38})`
                      : "rgba(32, 38, 48, 0.78)";
                  return (
                    <button
                      key={String(tile.date)}
                      data-calendar-day={String(tile.date)}
                      className={`calendar-day-cell ${selectedDay === tile.date ? "selected" : ""}`}
                      style={{ background, borderColor, borderWidth: 2 } as CSSProperties}
                      onClick={() => setSelectedDay(String(tile.date))}
                    >
                      <div className="calendar-day-number">{String(tile.date).slice(-2)}</div>
                      <div className={`calendar-day-pnl ${pnlTone(pnlValue)}`}>{renderPnlValue(pnlValue)}</div>
                      <div className="calendar-day-meta">{formatShortNumber(tile.trade_count)} trades</div>
                      <div className="calendar-day-meta">{formatShortNumber(tile.win_count)}W / {formatShortNumber(tile.loss_count)}L</div>
                      {asArray<JsonRecord>(tile.system_quality_markers).length ? (
                        <div className="calendar-day-markers">
                          {asArray<JsonRecord>(tile.system_quality_markers).slice(0, 3).map((marker, index) => (
                            <span key={`${String(tile.date)}-${String(marker.code ?? index)}`} className={`calendar-day-marker ${alertTone(marker.severity)}`} title={String(marker.code ?? marker.label ?? "")}>
                              {compactCalendarMarkerCode(marker.code ?? marker.label)}
                            </span>
                          ))}
                        </div>
                      ) : null}
                    </button>
                  );
                })}
              </div>
            </div>
          </>
        ) : null}
      </Section>

      {available ? (
        <Section title="Calendar Drilldown" subtitle="Selected day trade list, attribution, incidents, EOD positions, and source artifacts">
          <div className="metric-grid compact" data-selected-calendar-day={selectedDay}>
            <MetricCard label="Date" value={formatValue(selectedTile.date)} />
            <MetricCard label="Estimated Total" value={renderPnlValue(selectedTile.daily_estimated_total_pnl)} tone={pnlTone(selectedTile.daily_estimated_total_pnl)} />
            <MetricCard label="Realized" value={renderPnlValue(selectedTile.realized_pnl)} tone={pnlTone(selectedTile.realized_pnl)} />
            <MetricCard label="Unrealized EOD" value={renderPnlValue(selectedTile.unrealized_eod_pnl)} tone={pnlTone(selectedTile.unrealized_eod_pnl)} />
            <MetricCard label="Trades" value={formatShortNumber(selectedTile.trade_count)} />
            <MetricCard label="Win / Loss" value={`${formatShortNumber(selectedTile.win_count)} / ${formatShortNumber(selectedTile.loss_count)}`} />
            <MetricCard label="Largest Winner" value={renderPnlValue(selectedTile.largest_winner)} tone={pnlTone(selectedTile.largest_winner)} />
            <MetricCard label="Largest Loser" value={renderPnlValue(selectedTile.largest_loser)} tone={pnlTone(selectedTile.largest_loser)} />
            <MetricCard label="Max DD" value={renderPnlValue(selectedTile.max_intraday_drawdown)} tone={pnlTone(selectedTile.max_intraday_drawdown)} />
            <MetricCard label="Sample Quality" value={formatValue(selectedTile.sample_quality)} tone={selectedTile.sample_quality === "CLEAN_STRATEGY_SAMPLE_DAY" ? "good" : selectedTile.sample_quality === "NO_RUNTIME_TRADES" ? "muted" : "warn"} />
          </div>
          <h3 className="subsection-title">System / Data-Quality Markers</h3>
          <DataTable
            rows={asArray<JsonRecord>(selectedTile.system_quality_markers)}
            emptyLabel="No system or data-quality markers for this day."
            columns={[
              { key: "severity", label: "Severity", render: (row) => <Badge label={formatValue(row.severity)} tone={alertTone(row.severity)} /> },
              { key: "code", label: "Code" },
              { key: "label", label: "Label" },
            ]}
          />
          <h3 className="subsection-title">Trades</h3>
          <DataTable
            rows={asArray<JsonRecord>(selectedDrilldown.trade_list)}
            emptyLabel="No trades are recorded for this day."
            columns={[
              { key: "strategy_id", label: "Strategy" },
              { key: "lane_id", label: "Lane" },
              { key: "symbol", label: "Symbol" },
              { key: "side", label: "Side" },
              { key: "entry_timestamp", label: "Entry", render: (row) => formatTimestamp(row.entry_timestamp) },
              { key: "exit_timestamp", label: "Exit", render: (row) => formatTimestamp(row.exit_timestamp) },
              { key: "realized_pnl", label: "Realized", render: (row) => renderPnlValue(row.realized_pnl) },
            ]}
          />
          <h3 className="subsection-title">Strategy / Lane Breakdown</h3>
          <DataTable
            rows={asArray<JsonRecord>(selectedDrilldown.strategy_lane_pnl_breakdown)}
            emptyLabel="No strategy/lane breakdown is recorded for this day."
            columns={[
              { key: "strategy_id", label: "Strategy" },
              { key: "lane_id", label: "Lane" },
              { key: "realized_pnl", label: "Realized", render: (row) => renderPnlValue(row.realized_pnl) },
              { key: "trade_count", label: "Trades" },
            ]}
          />
          <h3 className="subsection-title">Source Artifacts</h3>
          <DataTable
            rows={asArray<string>(selectedDrilldown.source_artifacts).map((artifactPath) => ({ path: artifactPath }))}
            emptyLabel="No source artifacts are recorded for this day."
            columns={[{ key: "path", label: "Path" }]}
          />
        </Section>
      ) : null}
    </>
  );
}

function compactCalendarMarkerCode(value: unknown): string {
  const normalized = String(value ?? "").trim().toUpperCase();
  if (!normalized) {
    return "MARK";
  }
  if (normalized.includes("REVIEW")) {
    return "REVIEW";
  }
  if (normalized.includes("ADOPT")) {
    return "ADOPT";
  }
  if (normalized.includes("COST")) {
    return "BASIS";
  }
  if (normalized.includes("STALE")) {
    return "STALE";
  }
  if (normalized.includes("MISMATCH")) {
    return "MISMATCH";
  }
  if (normalized.includes("OPEN_ORDER")) {
    return "ORDER";
  }
  return normalized.split(/[^A-Z0-9]+/).filter(Boolean)[0]?.slice(0, 8) || "MARK";
}

function alertTone(value: unknown): Tone {
  const normalized = String(value ?? "").toUpperCase();
  if (normalized === "RED" || normalized === "DANGER" || normalized === "ERROR") {
    return "danger";
  }
  if (normalized === "YELLOW" || normalized === "WARN" || normalized === "WARNING") {
    return "warn";
  }
  if (normalized === "GREEN" || normalized === "OK" || normalized === "FRESH") {
    return "good";
  }
  return "muted";
}
