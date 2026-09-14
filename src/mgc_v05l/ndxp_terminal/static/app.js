"use strict";

const ui = Object.fromEntries(Array.from(document.querySelectorAll("[id]")).map((el) => [el.id, el]));
let state = null;
let optionType = "CALL";
let selectedShort = null;
let selectedLong = null;
let lastHeartbeat = performance.now();
let lastStateReceived = performance.now();

const money = (value) => value == null ? "—" : new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 }).format(Number(value));
const number = (value, digits = 2) => value == null ? "—" : Number(value).toLocaleString("en-US", { minimumFractionDigits: digits, maximumFractionDigits: digits });
const age = (ms) => ms == null ? "—" : ms < 1000 ? `${Math.round(ms)} ms` : `${(ms / 1000).toFixed(1)} s`;

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
  try {
    await api("/api/client-heartbeat", { method: "POST", body: JSON.stringify({ interval_ms: gap, state_response_age_ms: now - lastStateReceived }) });
  } catch (_) {}
}

function render() {
  const market = state.market || {};
  const broker = state.broker || {};
  const diagnostics = state.diagnostics || {};
  ui.clock.textContent = new Date(state.generated_at).toLocaleTimeString();
  ui.transmission.textContent = state.transmission.label;
  ui.spot.textContent = number(market.spot);
  ui["quote-age"].textContent = age(diagnostics.quote_source_age_ms);
  ui["market-latency"].textContent = age(diagnostics.market_latency_ms);
  ui["broker-latency"].textContent = age(diagnostics.broker_latency_ms);
  ui.diagnostic.textContent = diagnostics.classification || "STARTING";
  ui.diagnostic.className = diagnostics.classification === "HEALTHY" ? "status-good" : diagnostics.classification?.includes("ERROR") || diagnostics.classification?.includes("STALL") ? "status-bad" : "status-warn";
  renderExpirations(market.expirations || [], market.selected_expiration);
  renderAccounts(broker.accounts || [], broker.selected_account_hash);
  renderChain((market.selected_chain || {})[optionType] || [], market.spot);
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

function renderChain(rows, spot) {
  const near = [...rows].sort((a, b) => Math.abs(a.strike - spot) - Math.abs(b.strike - spot)).slice(0, 26).sort((a, b) => a.strike - b.strike);
  ui.chain.replaceChildren(...near.map((row) => {
    const node = document.createElement("div");
    node.className = `chain-row mono${Math.abs(row.strike - spot) <= 10 ? " near" : ""}`;
    [number(row.strike, 0), number(row.bid), number(row.ask), number(row.mid), number(row.delta, 3), number(row.gamma, 3), number(row.theta, 2)].forEach((value) => {
      const span = document.createElement("span"); span.textContent = value; node.appendChild(span);
    });
    const button = document.createElement("button"); button.textContent = "Sell"; button.addEventListener("click", () => chooseShort(row, rows)); node.appendChild(button);
    return node;
  }));
}

function chooseShort(row, rows) {
  const longStrike = row.strike + (optionType === "CALL" ? 10 : -10);
  const pair = rows.find((candidate) => Number(candidate.strike) === Number(longStrike));
  if (!pair) return showNotice("The protective 10-point leg is not present in the current chain.", true);
  selectedShort = row; selectedLong = pair;
  ui["short-leg"].textContent = `${row.symbol} @ ${number(row.strike, 0)}`;
  ui["long-leg"].textContent = `${pair.symbol} @ ${number(pair.strike, 0)}`;
  const natural = Math.max(0.05, Number(row.bid || 0) - Number(pair.ask || 0));
  ui.credit.value = natural.toFixed(2);
  ui.reviewed.checked = false;
  ui["preview-result"].textContent = "Spread constructed. Review the risk and build the Schwab order preview.";
  calculateRisk();
}

function calculateRisk() {
  const quantity = Number(ui.quantity.value || 0);
  const credit = Number(ui.credit.value || 0);
  const gross = quantity * 10 * 100;
  ui.premium.textContent = money(quantity * credit * 100);
  ui["gross-risk"].textContent = money(gross);
  ui["max-loss"].textContent = money(gross - quantity * credit * 100);
  ui.distance.textContent = selectedShort && state?.market?.spot != null ? `${number(Math.abs(selectedShort.strike - state.market.spot))} pts` : "—";
}

function orderPayload() {
  return {
    account_hash: ui.account.value,
    short_symbol: selectedShort?.symbol || "",
    long_symbol: selectedLong?.symbol || "",
    quantity: Number(ui.quantity.value),
    net_credit: ui.credit.value,
    duration: "DAY",
    session: "NORMAL"
  };
}

async function preview() {
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
  const rows = [
    ["UI heartbeat gap", age(d.client_gap_ms)],
    ["Worker scheduling gap", age(d.worker_gap_ms)],
    ["Schwab market response", age(d.market_latency_ms)],
    ["Schwab broker response", age(d.broker_latency_ms)],
    ["Successful poll age", age(d.market_poll_age_ms)],
    ["Quote source age", age(d.quote_source_age_ms)],
  ];
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

async function select() {
  selectedShort = null; selectedLong = null;
  ui["short-leg"].textContent = "Choose a strike"; ui["long-leg"].textContent = "Paired automatically";
  await api("/api/selection", { method: "POST", body: JSON.stringify({ expiration: ui.expiration.value, option_type: optionType }) });
  await refresh();
}

function showNotice(message, error = false) { ui.notice.textContent = message; ui.notice.className = `notice${error ? " error" : ""}`; }
function hideNotice() { ui.notice.className = "notice hidden"; }

ui.calls.addEventListener("click", () => { optionType = "CALL"; ui.calls.className = "active"; ui.puts.className = ""; select(); });
ui.puts.addEventListener("click", () => { optionType = "PUT"; ui.puts.className = "active"; ui.calls.className = ""; select(); });
ui.expiration.addEventListener("change", select);
ui.quantity.addEventListener("input", calculateRisk); ui.credit.addEventListener("input", calculateRisk);
ui.preview.addEventListener("click", preview);
ui.submit.addEventListener("click", () => lockedAction("submit", orderPayload()));
ui["access-check"].addEventListener("click", async () => {
  try { showNotice("Running read-only Schwab account and NDX chain checks…"); const result = await api("/api/access-check"); showNotice(`Access verified: ${JSON.stringify(result)}`); }
  catch (error) { showNotice(`Access check failed: ${error.message}`, true); }
});

refresh(); heartbeat(); setInterval(refresh, 1000); setInterval(heartbeat, 1000);
