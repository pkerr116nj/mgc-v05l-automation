import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const prototypeRoot = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(prototypeRoot, "../../..");
const listenerStatusPath = path.join(
  repoRoot,
  "outputs/reports/phase1_databento_live_runtime_candles/latest_phase1_databento_live_listener_status.json",
);
const candleRoot = path.join(repoRoot, "outputs/track_b_execution_core/phase1_runtime_market_data");
const outputPath = path.join(prototypeRoot, "market_tape_snapshot.generated.mjs");
const defaultSymbols = ["ES", "MES", "NQ", "MNQ", "GC", "MGC", "ZT", "ZF", "ZN", "ZB"];

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function asNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function formatPrice(value) {
  const number = asNumber(value);
  if (number === null) return "UNKNOWN";
  return number.toLocaleString("en-US", { maximumFractionDigits: 4 });
}

function signed(value, digits = 2) {
  const number = asNumber(value);
  if (number === null) return "UNKNOWN";
  const prefix = number >= 0 ? "+" : "";
  return `${prefix}${number.toFixed(digits)}`;
}

function statusForRow(row, lagSeconds) {
  if (row?.realtime_feed_confirmed !== true) return "STALE";
  if (row?.stream_caught_up !== true) return "STALE";
  if (lagSeconds === null) return "UNKNOWN";
  return lagSeconds <= 180 ? "OPEN" : "STALE";
}

function latestCompletedBars(symbol) {
  const artifactPath = path.join(candleRoot, symbol, "1m", "latest_runtime_candles.json");
  const payload = readJson(artifactPath);
  const bars = Array.isArray(payload.bars) ? payload.bars.filter((bar) => bar && bar.completed === true) : [];
  return { artifactPath, payload, bars };
}

function buildRow(symbol, statusRowBySymbol) {
  const statusRow = statusRowBySymbol.get(symbol) || {};
  const lagSeconds = asNumber(statusRow.stream_lag_seconds);
  const { artifactPath, bars } = latestCompletedBars(symbol);
  const latest = bars.at(-1) || {};
  const previous = bars.at(-2) || latest;
  const last = asNumber(latest.close);
  const prior = asNumber(previous.close);
  const changeAbs = last !== null && prior !== null ? last - prior : null;
  const changePct = last !== null && prior !== null && prior !== 0 ? (changeAbs / prior) * 100 : null;
  return {
    symbol,
    display_symbol: symbol,
    asset_type: ["BTC", "ETH", "MBT", "MET"].includes(symbol) ? "CRYPTO_FUTURES" : "FUTURES",
    group: "Track B Phase-1",
    last: formatPrice(last),
    changeAbs: signed(changeAbs),
    changePct: changePct === null ? "UNKNOWN" : `${signed(changePct, 3)}%`,
    bid: null,
    ask: null,
    market_status: statusForRow(statusRow, lagSeconds),
    source_timestamp: latest.bar_end || latest.timestamp || null,
    freshness: lagSeconds === null ? "UNKNOWN" : `${Math.round(lagSeconds)}s`,
    source_id: "phase1_runtime_market_data_latest_1m",
    source_artifact: path.relative(repoRoot, artifactPath),
  };
}

function buildSnapshot() {
  const status = readJson(listenerStatusPath);
  const statusRows = Array.isArray(status.rows) ? status.rows : [];
  const statusRowBySymbol = new Map(statusRows.map((row) => [String(row.symbol || "").toUpperCase(), row]));
  const watchlist = defaultSymbols.filter((symbol) => statusRowBySymbol.has(symbol));
  const rows = watchlist.map((symbol) => buildRow(symbol, statusRowBySymbol));
  return {
    schema_version: "observatory_market_tape_snapshot_v1",
    generated_at: status.generated_at || status.latest_record_at || null,
    model_status: rows.some((row) => row.market_status === "STALE") ? "VALID_WITH_WARNINGS" : "READY",
    source_kind: "PHASE1_COMPLETED_1M_CANDLE",
    source_artifacts: {
      listener_status: path.relative(repoRoot, listenerStatusPath),
      candle_root: path.relative(repoRoot, candleRoot),
    },
    freshness: {
      listener_generated_at: status.generated_at || null,
      latest_durable_completed_bar_ts: status.latest_durable_completed_bar_ts || null,
      current_lag_seconds: status.current_lag_seconds ?? null,
    },
    guardrails: {
      display_only: true,
      broker_authority: false,
      runtime_authority: false,
      trading_gate: false,
      strategy_input: false,
    },
    rows,
  };
}

const snapshot = buildSnapshot();
fs.writeFileSync(
  outputPath,
  `export const marketTapeSnapshot = ${JSON.stringify(snapshot, null, 2)};\n`,
  "utf8",
);
console.log(JSON.stringify({ ok: true, output: path.relative(repoRoot, outputPath), rows: snapshot.rows.length }, null, 2));
