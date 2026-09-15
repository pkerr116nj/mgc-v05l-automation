"use strict";

const ui = Object.fromEntries(Array.from(document.querySelectorAll("[id]")).map((el) => [el.id, el]));
const columnDefinitions = [
  ["last", "Last", 2], ["percent_change", "% Chng", 2], ["mark", "Mark", 2], ["bid", "Bid", 2], ["ask", "Ask", 2], ["net_change", "Net Chng", 2],
  ["spread_delta", "Derived Δ", 4], ["theta", "Derived Θ", 4], ["gamma", "Derived Γ", 4], ["iv", "Short IV", 2],
  ["volume", "Volume", 0], ["open_interest", "Open Int", 0],
];
const defaultColumns = ["last", "percent_change", "mark", "bid", "ask", "net_change", "spread_delta", "theta", "gamma", "iv"];
let visibleColumns = loadColumns();
let state = null;
let selectedShort = null;
let selectedLong = null;
let selectedAction = "OPEN";
let selectedMetrics = null;
let lastHeartbeat = performance.now();
let lastStateReceived = performance.now();

const money = (value) => value == null || !Number.isFinite(Number(value)) ? "—" : new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 }).format(Number(value));
const number = (value, digits = 2) => value == null || !Number.isFinite(Number(value)) ? "—" : Number(value).toLocaleString("en-US", { minimumFractionDigits: digits, maximumFractionDigits: digits });
const age = (ms) => ms == null ? "—" : ms < 1000 ? `${Math.round(ms)} ms` : `${(ms / 1000).toFixed(1)} s`;

function loadColumns() {
  try {
    const saved = JSON.parse(localStorage.getItem("ndxp-chain-columns") || "null");
    const migrated = Array.isArray(saved) ? saved.map((key) => key === "delta" ? "spread_delta" : key) : [];
    const valid = migrated.filter((key) => columnDefinitions.some(([candidate]) => candidate === key));
    return valid.includes("bid") ? valid : defaultColumns;
  } catch (_) { return defaultColumns; }
}

async function api(path, options = {}) {
  const response = await fetch(path, { cache: "no-store", headers: { "Content-Type": "application/json" }, ...options });
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
  return payload;
}

async function refresh() {
  try {
    state = await api("/api/state");
    lastStateReceived = performance.now();
    render();
  } catch (error) {
    showNotice(`Terminal state unavailable: ${error.message}`, true);
    ui.diagnostic.textContent = "LOCAL UI / SERVER ERROR";
    ui.diagnostic.className = "status-bad";
  }
}

async function heartbeat() {
  const now = performance.now();
  const gap = now - lastHeartbeat;
  lastHeartbeat = now;
  try { await api("/api/client-heartbeat", { method: "POST", body: JSON.stringify({ interval_ms: gap, state_response_age_ms: now - lastStateReceived }) }); }
  catch (_) {}
}

function render() {
  const market = state.market || {};
  const broker = state.broker || {};
  const diagnostics = state.diagnostics || {};
  ui.clock.textContent = new Date(state.generated_at).toLocaleTimeString([], { hour: "numeric", minute: "2-digit", second: "2-digit" });
  ui.transmission.textContent = state.transmission.label;
  ui.spot.textContent = number(market.spot, 2);
  ui["quote-age"].textContent = age(diagnostics.quote_source_age_ms);
  ui["market-latency"].textContent = age(diagnostics.market_latency_ms);
  ui["broker-latency"].textContent = age(diagnostics.broker_latency_ms);
  ui.diagnostic.textContent = diagnostics.classification || "STARTING";
  ui.diagnostic.className = diagnostics.classification === "HEALTHY" ? "status-good" : diagnostics.classification?.includes("ERROR") || diagnostics.classification?.includes("STALL") ? "status-bad" : "status-warn";
  renderExpirations(market.expirations || [], market.selected_expiration);
  renderAccounts(broker.accounts || [], broker.selected_account_hash);
  renderChain(market.selected_chain || {}, market.spot);
  renderPositions(broker.positions || []);
  renderOrders(broker.working_orders || []);
  renderDiagnostics(diagnostics);
  calculateRisk();
  const errors = Object.values(state.errors || {}).filter(Boolean);
  if (errors.length) showNotice(errors.join(" | "), true); else hideNotice();
}

function renderExpirations(expirations, selected) {
  const current = ui.expiration.value;
  ui.expiration.replaceChildren(...expirations.map((expiration) => new Option(expiration, expiration)));
  ui.expiration.value = expirations.includes(current) ? current : selected || expirations[0] || "";
}

function renderAccounts(accounts, selected) {
  const current = ui.account.value;
  ui.account.replaceChildren(...accounts.map((row) => new Option(`${row.number_masked} · ${row.type || "Account"}`, row.hash)));
  ui.account.value = accounts.some((row) => row.hash === current) ? current : selected || accounts[0]?.hash || "";
}

function verticalRows(chain) {
  const calls = new Map((chain.CALL || []).map((row) => [Number(row.strike), row]));
  const puts = new Map((chain.PUT || []).map((row) => [Number(row.strike), row]));
  return [...new Set([...calls.keys(), ...puts.keys()])].sort((a, b) => a - b).flatMap((low) => {
    const high = low + 10;
    const callShort = calls.get(low), callLong = calls.get(high);
    const putLong = puts.get(low), putShort = puts.get(high);
    if (!callShort && !putShort) return [];
    return [{
      low, high,
      call: callShort && callLong ? { short: callShort, long: callLong, metrics: spreadMetrics(callShort, callLong) } : null,
      put: putShort && putLong ? { short: putShort, long: putLong, metrics: spreadMetrics(putShort, putLong) } : null,
    }];
  });
}

function difference(short, long, key) {
  const a = Number(short?.[key]), b = Number(long?.[key]);
  return Number.isFinite(a) && Number.isFinite(b) ? a - b : null;
}

function spreadMetrics(short, long) {
  const bid = Number.isFinite(Number(short.bid)) && Number.isFinite(Number(long.ask)) ? Number(short.bid) - Number(long.ask) : null;
  const ask = Number.isFinite(Number(short.ask)) && Number.isFinite(Number(long.bid)) ? Number(short.ask) - Number(long.bid) : null;
  const legMark = difference(short, long, "mark");
  const mark = Number.isFinite(bid) && Number.isFinite(ask) ? (bid + ask) / 2 : legMark;
  return {
    last: difference(short, long, "last"), percent_change: difference(short, long, "percent_change"),
    mark, bid, ask, net_change: difference(short, long, "net_change"),
    spread_delta: difference(long, short, "delta"), theta: difference(long, short, "theta"), gamma: difference(long, short, "gamma"),
    iv: short.iv, volume: short.volume, open_interest: short.open_interest,
  };
}

function gridTemplate() {
  const side = visibleColumns.map((key) => key === "open_interest" ? "94px" : "78px").join(" ");
  return `${side} 96px ${side}`;
}

function renderChain(chain, spot) {
  const rows = verticalRows(chain);
  ui["chain-table"].style.gridTemplateColumns = gridTemplate();
  const fragments = [];
  const callGroup = cell("CALLS", "group-head calls-head"); callGroup.style.gridColumn = `span ${visibleColumns.length}`;
  const strikeGroup = cell("10-POINT", "group-head strike-head");
  const putGroup = cell("PUTS", "group-head puts-head"); putGroup.style.gridColumn = `span ${visibleColumns.length}`;
  fragments.push(callGroup, strikeGroup, putGroup);
  for (const side of ["call", "strike", "put"]) {
    const columns = side === "strike" ? [["strike", "Strikes"]] : visibleColumns.map((key) => columnDefinitions.find(([candidate]) => candidate === key));
    for (const [, label] of columns) fragments.push(cell(label, `column-head ${side === "strike" ? "strike-cell" : ""}`));
  }
  for (const row of rows) {
    const near = Number.isFinite(Number(spot)) && row.low <= spot && row.high >= spot;
    for (const key of visibleColumns) fragments.push(metricCell(row.call, key, near, "CALL"));
    fragments.push(cell(`${number(row.low, 0)} / ${number(row.high, 0)}`, `strike-cell spread-strikes${near ? " near" : ""}`));
    for (const key of visibleColumns) fragments.push(metricCell(row.put, key, near, "PUT"));
  }
  if (!rows.length) {
    const empty = cell("Waiting for the selected Schwab option chain…", "chain-empty");
    empty.style.gridColumn = "1 / -1"; fragments.push(empty);
  }
  ui["chain-table"].replaceChildren(...fragments);
}

function cell(text, className = "") {
  const node = document.createElement("div"); node.className = className; node.textContent = text; node.setAttribute("role", "cell"); return node;
}

function metricCell(spread, key, near, side) {
  const definition = columnDefinitions.find(([candidate]) => candidate === key);
  const value = spread?.metrics?.[key];
  const formatted = key === "iv" && value != null ? `${number(value, definition[2])}%` : number(value, definition[2]);
  if ((key === "bid" || key === "ask") && spread) {
    const button = document.createElement("button");
    const opening = key === "bid";
    button.className = `metric ${opening ? "bid-action" : "ask-action"}${near ? " near" : ""}`;
    button.textContent = formatted;
    button.title = opening ? `Sell to open ${side.toLowerCase()} credit spread` : `Buy to close ${side.toLowerCase()} credit spread`;
    button.disabled = value == null || Number(value) <= 0;
    button.addEventListener("click", () => openTicket(opening ? "OPEN" : "CLOSE", side, spread));
    button.setAttribute("role", "cell");
    return button;
  }
  return cell(formatted, `metric${near ? " near" : ""}`);
}

function openTicket(action, side, spread) {
  selectedAction = action;
  selectedMetrics = spread.metrics;
  selectedShort = spread.short; selectedLong = spread.long;
  const opening = action === "OPEN";
  ui["ticket-side"].textContent = `${side} · ${opening ? "SELL TO OPEN" : "BUY TO CLOSE"}`;
  ui["ticket-title"].textContent = `${opening ? "Sell" : "Buy"} ${side.toLowerCase()} vertical`;
  ui["ticket-market"].textContent = `Bid ${number(spread.metrics.bid)} · Mid ${number(spread.metrics.mark)} · Ask ${number(spread.metrics.ask)}`;
  ui["short-instruction"].textContent = opening ? "SELL TO OPEN" : "BUY TO CLOSE";
  ui["long-instruction"].textContent = opening ? "BUY TO OPEN" : "SELL TO CLOSE";
  ui["short-leg"].textContent = `${selectedShort.symbol} · ${number(selectedShort.strike, 0)}`;
  ui["long-leg"].textContent = `${selectedLong.symbol} · ${number(selectedLong.strike, 0)}`;
  ui.quantity.value = "20";
  ui.credit.value = Math.max(0.05, Number(spread.metrics.mark || 0)).toFixed(2);
  ui.reviewed.checked = false;
  ui["preview-result"].textContent = `${opening ? "Opening credit" : "Closing debit"} ticket constructed at the displayed mid. Review before building the Schwab payload.`;
  calculateRisk();
  ui["ticket-dialog"].showModal();
}

function calculateRisk() {
  const quantity = Number(ui.quantity.value || 0), credit = Number(ui.credit.value || 0), gross = quantity * 10 * 100;
  ui.premium.textContent = money(quantity * credit * 100);
  ui["gross-risk"].textContent = money(gross);
  ui["premium-label"].textContent = selectedAction === "OPEN" ? "Premium" : "Closing debit";
  ui["max-loss-label"].textContent = selectedAction === "OPEN" ? "Maximum loss" : "Position effect";
  ui["max-loss"].textContent = selectedAction === "OPEN" ? money(gross - quantity * credit * 100) : "Reduces risk";
  ui.distance.textContent = selectedShort && state?.market?.spot != null ? `${number(Math.abs(selectedShort.strike - state.market.spot))} pts` : "—";
  const perSpreadDelta = selectedMetrics?.spread_delta;
  ui["spread-delta"].textContent = number(perSpreadDelta, 4);
  ui["position-delta"].textContent = perSpreadDelta == null ? "—" : number(Number(perSpreadDelta) * quantity, 2);
}

function orderPayload() {
  return { account_hash: ui.account.value, short_symbol: selectedShort?.symbol || "", long_symbol: selectedLong?.symbol || "", quantity: Number(ui.quantity.value), limit_price: ui.credit.value, action: selectedAction, duration: "DAY", session: "NORMAL" };
}

async function preview() {
  if (!selectedShort || !selectedLong) return showNotice("Tap a call or put bid first.", true);
  if (!ui.reviewed.checked) return showNotice("Review the order details and tick the confirmation box before building the preview.", true);
  try {
    const result = await api("/api/preview", { method: "POST", body: JSON.stringify(orderPayload()) });
    ui["preview-result"].textContent = JSON.stringify(result, null, 2);
    showNotice("Order preview built. No broker mutation was attempted.");
  } catch (error) { showNotice(error.message, true); }
}

function renderPositions(rows) {
  if (!rows.length) { ui.positions.className = "empty"; ui.positions.textContent = "No NDX/NDXP positions in current Schwab account truth."; return; }
  ui.positions.className = "";
  ui.positions.replaceChildren(...rows.map((row) => dataRow(row.symbol, `${row.long_quantity || 0} long · ${row.short_quantity || 0} short · avg ${row.average_price ?? "—"}`)));
}

function renderOrders(rows) {
  if (!rows.length) { ui.orders.className = "empty"; ui.orders.textContent = "No working NDX/NDXP orders."; return; }
  ui.orders.className = "";
  ui.orders.replaceChildren(...rows.map((row) => {
    const node = dataRow(`#${row.order_id} · ${row.order_type} ${row.price ?? ""}`, row.legs.map((leg) => `${leg.instruction} ${leg.quantity} ${leg.symbol}`).join(" · "));
    const actions = document.createElement("div"); actions.className = "actions";
    const cancel = document.createElement("button"); cancel.textContent = "Cancel (locked)"; cancel.addEventListener("click", () => lockedAction("cancel", { account_hash: ui.account.value, broker_order_id: row.order_id }));
    const replace = document.createElement("button"); replace.textContent = "Replace (locked)"; replace.addEventListener("click", () => lockedAction("replace", { ...orderPayload(), broker_order_id: row.order_id }));
    actions.append(cancel, replace); node.appendChild(actions); return node;
  }));
}

function dataRow(title, detail) {
  const node = document.createElement("div"); node.className = "data-row";
  const strong = document.createElement("strong"); strong.textContent = title;
  const small = document.createElement("small"); small.textContent = detail;
  node.append(strong, small); return node;
}

function renderDiagnostics(d) {
  const rows = [["UI heartbeat gap", age(d.client_gap_ms)], ["Worker scheduling gap", age(d.worker_gap_ms)], ["Schwab market response", age(d.market_latency_ms)], ["Schwab broker response", age(d.broker_latency_ms)], ["Successful poll age", age(d.market_poll_age_ms)], ["Quote source age", age(d.quote_source_age_ms)]];
  ui["diagnostic-detail"].replaceChildren(...rows.flatMap(([label, value]) => {
    const dt = document.createElement("dt"); dt.textContent = label;
    const dd = document.createElement("dd"); dd.textContent = value;
    return [dt, dd];
  }));
}

async function lockedAction(action, payload) {
  try { await api(`/api/${action}`, { method: "POST", body: JSON.stringify(payload) }); }
  catch (error) { showNotice(error.message, true); }
}

async function selectExpiration() {
  selectedShort = null; selectedLong = null; selectedAction = "OPEN"; selectedMetrics = null;
  await api("/api/selection", { method: "POST", body: JSON.stringify({ expiration: ui.expiration.value, option_type: "CALL" }) });
  await refresh();
}

function renderColumnOptions() {
  ui["column-options"].replaceChildren(...columnDefinitions.map(([key, label]) => {
    const wrapper = document.createElement("label"); wrapper.className = "column-choice";
    const input = document.createElement("input"); input.type = "checkbox"; input.checked = visibleColumns.includes(key); input.disabled = key === "bid";
    input.addEventListener("change", () => {
      visibleColumns = input.checked ? [...visibleColumns, key] : visibleColumns.filter((candidate) => candidate !== key);
      visibleColumns.sort((a, b) => columnDefinitions.findIndex(([candidate]) => candidate === a) - columnDefinitions.findIndex(([candidate]) => candidate === b));
      localStorage.setItem("ndxp-chain-columns", JSON.stringify(visibleColumns));
      renderChain(state?.market?.selected_chain || {}, state?.market?.spot);
    });
    wrapper.append(input, document.createTextNode(label)); return wrapper;
  }));
}

function showNotice(message, error = false) { ui.notice.textContent = message; ui.notice.className = `notice${error ? " error" : ""}`; }
function hideNotice() { ui.notice.className = "notice hidden"; }

ui.expiration.addEventListener("change", selectExpiration);
ui.quantity.addEventListener("input", calculateRisk); ui.credit.addEventListener("input", calculateRisk);
ui.preview.addEventListener("click", preview); ui.submit.addEventListener("click", () => lockedAction("submit", orderPayload()));
ui["columns-button"].addEventListener("click", () => ui["columns-dialog"].showModal());
document.querySelectorAll("[data-jump]").forEach((button) => button.addEventListener("click", () => document.getElementById(button.dataset.jump).scrollIntoView({ behavior: "smooth" })));
ui["access-check"].addEventListener("click", async () => {
  try { showNotice("Running read-only Schwab account and NDX chain checks…"); const result = await api("/api/access-check"); showNotice(`Access verified: ${JSON.stringify(result)}`); }
  catch (error) { showNotice(`Access check failed: ${error.message}`, true); }
});

renderColumnOptions();
refresh(); heartbeat(); setInterval(refresh, 1000); setInterval(heartbeat, 1000);
