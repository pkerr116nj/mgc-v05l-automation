"use strict";

const ui = Object.fromEntries(Array.from(document.querySelectorAll("[id]")).map((el) => [el.id, el]));
const columnDefinitions = [
  ["last", "Last", 2], ["percent_change", "% Chng", 2], ["mark", "Mark", 2], ["bid", "Bid", 2], ["ask", "Ask", 2], ["net_change", "Net Chng", 2],
  ["breakeven_distance", "BE Dist", 1], ["em_multiple", "BE / EM", 2], ["probability_beyond_breakeven", "Beyond BE", 1],
  ["probability_beyond_short", "Beyond Short", 1], ["credit_to_risk", "Credit / Risk", 1], ["market_width", "Mkt Width", 2],
  ["spread_delta", "Derived Δ", 4], ["theta", "Derived Θ", 4], ["gamma", "Derived Γ", 4], ["iv", "Short IV", 2],
  ["volume", "Volume", 0], ["open_interest", "Open Int", 0],
];
const defaultColumns = ["last", "mark", "bid", "ask", "breakeven_distance", "em_multiple", "probability_beyond_breakeven", "credit_to_risk", "spread_delta", "iv"];
const minimumStrikesEachSide = 25;
let visibleColumns = loadColumns();
let opportunityFilters = loadFilters();
let state = null;
let selectedShort = null;
let selectedLong = null;
let selectedAction = "OPEN";
let selectedMetrics = null;
let lastHeartbeat = performance.now();
let lastStateReceived = performance.now();
let centeredExpiration = null;

const money = (value) => value == null || !Number.isFinite(Number(value)) ? "—" : new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 }).format(Number(value));
const number = (value, digits = 2) => value == null || !Number.isFinite(Number(value)) ? "—" : Number(value).toLocaleString("en-US", { minimumFractionDigits: digits, maximumFractionDigits: digits });
const age = (ms) => ms == null ? "—" : ms < 1000 ? `${Math.round(ms)} ms` : `${(ms / 1000).toFixed(1)} s`;
const percent = (value, digits = 1) => value == null || !Number.isFinite(Number(value)) ? "—" : `${(Number(value) * 100).toFixed(digits)}%`;

function loadColumns() {
  try {
    const saved = JSON.parse(localStorage.getItem("ndxp-chain-columns-v2") || "null");
    const migrated = Array.isArray(saved) ? saved.map((key) => key === "delta" ? "spread_delta" : key) : [];
    const valid = migrated.filter((key) => columnDefinitions.some(([candidate]) => candidate === key));
    return valid.includes("bid") ? valid : defaultColumns;
  } catch (_) { return defaultColumns; }
}

function loadFilters() {
  const defaults = { em_multiple: 1.0, min_credit: 1.0, credit_to_risk: 0.10, max_market_width: 0.75 };
  try {
    const saved = JSON.parse(localStorage.getItem("ndxp-opportunity-filters") || "null");
    if (!saved || typeof saved !== "object") return defaults;
    return Object.fromEntries(Object.entries(defaults).map(([key, fallback]) => {
      const value = Number(saved[key]); return [key, Number.isFinite(value) && value >= 0 ? value : fallback];
    }));
  } catch (_) { return defaults; }
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
  if (state.mode === "DEMO") {
    ui["mode-banner"].textContent = "SYNTHETIC DEMO DATA · NOT CONNECTED TO DATABENTO OR SCHWAB MARKET DATA";
    ui["mode-banner"].className = "mode-banner";
  } else {
    ui["mode-banner"].className = "mode-banner hidden";
  }
  ui.spot.textContent = number(market.spot, 2);
  ui["quote-age"].textContent = age(diagnostics.quote_source_age_ms);
  ui["market-latency"].textContent = age(diagnostics.market_latency_ms);
  const databento = market.databento;
  ui["option-source-label"].textContent = databento ? "Databento OPRA NBBO" : "Option market · Schwab";
  ui["option-feed-status"].textContent = databento
    ? `${databento.selected_quote_count || 0} / ${databento.target_symbol_count || 0} quoted`
    : "Schwab chain quotes";
  ui["option-feed-status"].title = databento
    ? `${databento.subscribed_symbol_count || 0} contracts subscribed in one shared session${databento.last_error ? ` · ${databento.last_error}` : ""}`
    : "Option quotes are supplied by the Schwab chain response.";
  ui["broker-latency"].textContent = age(diagnostics.broker_latency_ms);
  ui.diagnostic.textContent = diagnostics.classification || "STARTING";
  ui.diagnostic.className = diagnostics.classification === "HEALTHY" ? "status-good" : diagnostics.classification?.includes("ERROR") || diagnostics.classification?.includes("STALL") ? "status-bad" : "status-warn";
  renderExpirations(market.expirations || [], market.selected_expiration);
  renderAccounts(broker.accounts || [], broker.selected_account_hash);
  renderAnalytics(market.analytics || {});
  renderChain(market.selected_chain || {}, market.spot, market.analytics || {});
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

function renderAnalytics(analytics) {
  const valid = analytics.status === "VALID";
  document.querySelector(".range-strip").classList.toggle("invalid", !valid);
  if (!valid) {
    ui["expected-range"].textContent = "Analytics unavailable";
    ui["expected-move"].textContent = analytics.reason || "Waiting for independently valid option mids.";
    ui["derived-atm-iv"].textContent = "—";
    ui["time-remaining"].textContent = "—";
    ui["secondary-ranges"].textContent = "—";
    ui["candidate-count"].textContent = "—";
    return;
  }
  const one = analytics.ranges?.["1.0"];
  const half = analytics.ranges?.["0.5"];
  const oneHalf = analytics.ranges?.["1.5"];
  ui["expected-range"].textContent = one ? `${number(one.lower, 0)} – ${number(one.upper, 0)}` : "—";
  ui["expected-move"].textContent = `±${number(analytics.expected_move, 1)} points · independently derived from option mids`;
  ui["derived-atm-iv"].textContent = `${number(analytics.atm_iv_percent, 2)}%`;
  ui["time-remaining"].textContent = formatDuration(analytics.seconds_remaining);
  ui["secondary-ranges"].textContent = half && oneHalf ? `0.5σ ${number(half.lower, 0)}–${number(half.upper, 0)} · 1.5σ ${number(oneHalf.lower, 0)}–${number(oneHalf.upper, 0)}` : "—";
  ui["expected-range"].title = `${analytics.method}; ${analytics.surface_points} fitted strikes; parity RMS ${number(analytics.parity_rms_error, 3)} points.`;
}

function formatDuration(seconds) {
  const total = Number(seconds);
  if (!Number.isFinite(total) || total < 0) return "—";
  const hours = Math.floor(total / 3600), minutes = Math.floor((total % 3600) / 60);
  return hours ? `${hours}h ${minutes}m` : `${minutes}m`;
}

function verticalRows(chain, analytics) {
  const calls = new Map((chain.CALL || []).map((row) => [Number(row.strike), row]));
  const puts = new Map((chain.PUT || []).map((row) => [Number(row.strike), row]));
  return [...new Set([...calls.keys(), ...puts.keys()])].sort((a, b) => a - b).flatMap((low) => {
    const high = low + 10;
    const callShort = calls.get(low), callLong = calls.get(high);
    const putLong = puts.get(low), putShort = puts.get(high);
    if (!callShort && !putShort) return [];
    return [{
      low, high,
      call: callShort && callLong ? { short: callShort, long: callLong, metrics: spreadMetrics(callShort, callLong, analytics?.spreads?.[callShort.symbol]) } : null,
      put: putShort && putLong ? { short: putShort, long: putLong, metrics: spreadMetrics(putShort, putLong, analytics?.spreads?.[putShort.symbol]) } : null,
    }];
  });
}

function difference(short, long, key) {
  const a = Number(short?.[key]), b = Number(long?.[key]);
  return Number.isFinite(a) && Number.isFinite(b) ? a - b : null;
}

function spreadMetrics(short, long, model = null) {
  const bid = Number.isFinite(Number(short.bid)) && Number.isFinite(Number(long.ask)) ? Number(short.bid) - Number(long.ask) : null;
  const ask = Number.isFinite(Number(short.ask)) && Number.isFinite(Number(long.bid)) ? Number(short.ask) - Number(long.bid) : null;
  const legMark = difference(short, long, "mark");
  const mark = Number.isFinite(bid) && Number.isFinite(ask) ? (bid + ask) / 2 : legMark;
  return {
    last: difference(short, long, "last"), percent_change: difference(short, long, "percent_change"),
    mark, bid, ask, net_change: difference(short, long, "net_change"),
    spread_delta: difference(long, short, "delta"), theta: difference(long, short, "theta"), gamma: difference(long, short, "gamma"),
    iv: short.iv, volume: short.volume, open_interest: short.open_interest,
    ...(model || {}),
  };
}

function gridTemplate() {
  const side = visibleColumns.map((key) => key === "open_interest" ? "94px" : "78px").join(" ");
  return `${side} 96px ${side}`;
}

function renderChain(chain, spot, analytics = {}) {
  const rows = verticalRows(chain, analytics);
  renderChainCoverage(chain, spot);
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
    const callOtm = row.call && Number(row.call.short.strike) > Number(spot);
    const putOtm = row.put && Number(row.put.short.strike) < Number(spot);
    for (const key of visibleColumns) fragments.push(metricCell(row.call, key, near, "CALL", callOtm));
    fragments.push(cell(`${number(row.low, 0)} / ${number(row.high, 0)}`, `strike-cell spread-strikes${near ? " near" : ""}`));
    for (const key of visibleColumns) fragments.push(metricCell(row.put, key, near, "PUT", putOtm));
  }
  if (!rows.length) {
    const empty = cell("Waiting for the selected option market…", "chain-empty");
    empty.style.gridColumn = "1 / -1"; fragments.push(empty);
  }
  ui["chain-table"].replaceChildren(...fragments);
  renderCandidateCount(rows, analytics);
  const expiration = state?.market?.selected_expiration || "";
  if (rows.length && expiration !== centeredExpiration) {
    centeredExpiration = expiration;
    requestAnimationFrame(() => ui["chain-table"].querySelector(".spread-strikes.near")?.scrollIntoView({ block: "center", inline: "nearest" }));
  }
}

function renderChainCoverage(chain, spot) {
  const calls = new Set((chain.CALL || []).map((row) => Number(row.strike)).filter(Number.isFinite));
  const commonStrikes = (chain.PUT || []).map((row) => Number(row.strike)).filter((strike) => Number.isFinite(strike) && calls.has(strike));
  const below = commonStrikes.filter((strike) => strike < Number(spot)).length;
  const above = commonStrikes.filter((strike) => strike > Number(spot)).length;
  const complete = below >= minimumStrikesEachSide && above >= minimumStrikesEachSide;
  ui["chain-coverage"].textContent = `${below} below · ${above} above${complete ? "" : " · LIMITED"}`;
  ui["chain-coverage"].className = `chain-coverage ${complete ? "status-good" : "status-warn"}`;
  ui["chain-coverage"].title = complete
    ? `At least ${minimumStrikesEachSide} call-and-put strike levels are available on each side of spot.`
    : `The current contract roster contains fewer than ${minimumStrikesEachSide} shared call-and-put strike levels on one or both sides of spot.`;
}

function renderCandidateCount(rows, analytics) {
  if (analytics.status !== "VALID") { ui["candidate-count"].textContent = "—"; return; }
  const spreads = rows.flatMap((row) => [row.call, row.put]).filter(Boolean);
  const states = spreads.map((spread) => opportunityState(spread.metrics));
  const qualified = states.filter((candidate) => candidate.qualified).length;
  const preferred = states.filter((candidate) => candidate.preferred).length;
  ui["candidate-count"].textContent = `${qualified} / ${preferred}`;
  ui["candidate-count"].title = `${qualified} spreads pass all transparent filters; ${preferred} of those have a $2.00–$2.50 opening mid.`;
}

function cell(text, className = "") {
  const node = document.createElement("div"); node.className = className; node.textContent = text; node.setAttribute("role", "cell"); return node;
}

function metricCell(spread, key, near, side, otm = false) {
  const definition = columnDefinitions.find(([candidate]) => candidate === key);
  const value = spread?.metrics?.[key];
  const formatted = formatMetric(key, value, definition[2]);
  const opportunity = opportunityState(spread?.metrics);
  if ((key === "bid" || key === "ask") && spread) {
    const button = document.createElement("button");
    const opening = key === "bid";
    button.className = `metric ${opening ? "bid-action" : "ask-action"}${near ? " near" : ""}${otm ? " otm" : ""}${opening && opportunity.qualified ? " qualified" : ""}${opening && opportunity.preferred ? " preferred" : ""}${opening && spread.metrics?.credit_band === "ELEVATED" ? " elevated" : ""}`;
    button.textContent = formatted;
    const moneyness = otm ? " · OTM" : "";
    button.title = opening ? `Sell to open ${side.toLowerCase()} credit spread${moneyness} · ${opportunity.label}` : `Buy to close ${side.toLowerCase()} credit spread${moneyness}`;
    button.disabled = value == null || Number(value) <= 0;
    button.addEventListener("click", () => openTicket(opening ? "OPEN" : "CLOSE", side, spread));
    button.setAttribute("role", "cell");
    return button;
  }
  return cell(formatted, `metric${near ? " near" : ""}${otm ? " otm" : ""}${opportunity.qualified ? " qualified" : ""}`);
}

function formatMetric(key, value, digits) {
  if (key === "iv" && value != null) return `${number(value, digits)}%`;
  if (["probability_beyond_breakeven", "probability_beyond_short", "credit_to_risk"].includes(key)) return percent(value, digits);
  if (key === "em_multiple" && value != null) return `${number(value, digits)}×`;
  return number(value, digits);
}

function opportunityState(metrics) {
  if (!metrics || metrics.em_multiple == null) return { qualified: false, preferred: false, label: "Model analytics unavailable" };
  const checks = [
    [Number(metrics.em_multiple) >= opportunityFilters.em_multiple, `BE/EM ≥ ${opportunityFilters.em_multiple.toFixed(1)}×`],
    [Number(metrics.opening_mid) >= opportunityFilters.min_credit, `credit ≥ ${opportunityFilters.min_credit.toFixed(2)}`],
    [Number(metrics.credit_to_risk) >= opportunityFilters.credit_to_risk, `credit/risk ≥ ${percent(opportunityFilters.credit_to_risk)}`],
    [Number(metrics.market_width) <= opportunityFilters.max_market_width, `width ≤ ${opportunityFilters.max_market_width.toFixed(2)}`],
  ];
  const failed = checks.filter(([passes]) => !passes).map(([, label]) => label);
  const qualified = failed.length === 0;
  return {
    qualified,
    preferred: qualified && metrics.credit_band === "PREFERRED",
    label: qualified ? `${metrics.credit_band === "PREFERRED" ? "Preferred" : "Qualified"}: all filters pass` : `Not highlighted: ${failed.join("; ")}`,
  };
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
  ui["ticket-breakeven"].textContent = number(selectedMetrics?.breakeven, 2);
  ui["ticket-em-multiple"].textContent = selectedMetrics?.em_multiple == null ? "—" : `${number(selectedMetrics.em_multiple, 2)}×`;
  ui["ticket-tail-probability"].textContent = percent(selectedMetrics?.probability_beyond_breakeven, 1);
  ui["ticket-credit-risk"].textContent = percent(selectedMetrics?.credit_to_risk, 1);
  if (selectedAction === "OPEN" && quantity > 0 && credit > 0) {
    const lowProfit = (credit - 1.40) * quantity * 100;
    const highProfit = (credit - 1.00) * quantity * 100;
    ui["target-profit"].textContent = `${money(lowProfit)} – ${money(highProfit)}`;
  } else {
    ui["target-profit"].textContent = "Opening credit required";
  }
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
  const rows = [["UI heartbeat gap", age(d.client_gap_ms)], ["Worker scheduling gap", age(d.worker_gap_ms)], ["Schwab roster / spot response", age(d.market_latency_ms)], ["Schwab broker response", age(d.broker_latency_ms)], ["Successful poll age", age(d.market_poll_age_ms)], ["Option quote source age", age(d.quote_source_age_ms)]];
  if (d.databento) {
    rows.push(["Databento session", d.databento.started ? "Connected" : "Starting"]);
    rows.push(["OPRA selected quotes", `${d.databento.selected_quote_count || 0} / ${d.databento.target_symbol_count || 0}`]);
  }
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
      localStorage.setItem("ndxp-chain-columns-v2", JSON.stringify(visibleColumns));
      renderChain(state?.market?.selected_chain || {}, state?.market?.spot, state?.market?.analytics || {});
    });
    wrapper.append(input, document.createTextNode(label)); return wrapper;
  }));
}

function populateFilters() {
  ui["filter-em"].value = opportunityFilters.em_multiple.toFixed(1);
  ui["filter-credit"].value = opportunityFilters.min_credit.toFixed(2);
  ui["filter-credit-risk"].value = opportunityFilters.credit_to_risk.toFixed(2);
  ui["filter-width"].value = opportunityFilters.max_market_width.toFixed(2);
}

function saveFilters() {
  const proposed = {
    em_multiple: Number(ui["filter-em"].value), min_credit: Number(ui["filter-credit"].value),
    credit_to_risk: Number(ui["filter-credit-risk"].value), max_market_width: Number(ui["filter-width"].value),
  };
  if (Object.values(proposed).some((value) => !Number.isFinite(value) || value < 0) || proposed.max_market_width <= 0) {
    showNotice("Opportunity filters must be valid non-negative numbers, and market width must be positive.", true);
    return false;
  }
  opportunityFilters = proposed;
  localStorage.setItem("ndxp-opportunity-filters", JSON.stringify(opportunityFilters));
  renderChain(state?.market?.selected_chain || {}, state?.market?.spot, state?.market?.analytics || {});
  return true;
}

function showNotice(message, error = false) { ui.notice.textContent = message; ui.notice.className = `notice${error ? " error" : ""}`; }
function hideNotice() { ui.notice.className = "notice hidden"; }

ui.expiration.addEventListener("change", selectExpiration);
ui.quantity.addEventListener("input", calculateRisk); ui.credit.addEventListener("input", calculateRisk);
ui.preview.addEventListener("click", preview); ui.submit.addEventListener("click", () => lockedAction("submit", orderPayload()));
ui["columns-button"].addEventListener("click", () => ui["columns-dialog"].showModal());
ui["filters-button"].addEventListener("click", () => { populateFilters(); ui["filters-dialog"].showModal(); });
ui["save-filters"].addEventListener("click", (event) => {
  if (!saveFilters()) event.preventDefault();
});
document.querySelectorAll("[data-jump]").forEach((button) => button.addEventListener("click", () => document.getElementById(button.dataset.jump).scrollIntoView({ behavior: "smooth" })));
ui["access-check"].addEventListener("click", async () => {
  try { showNotice("Running read-only Schwab account and NDX chain checks…"); const result = await api("/api/access-check"); showNotice(`Access verified: ${JSON.stringify(result)}`); }
  catch (error) { showNotice(`Access check failed: ${error.message}`, true); }
});

renderColumnOptions();
refresh(); heartbeat(); setInterval(refresh, 1000); setInterval(heartbeat, 1000);
