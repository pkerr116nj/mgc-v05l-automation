"use strict";

const ui = Object.fromEntries(Array.from(document.querySelectorAll("[id]")).map((el) => [el.id, el]));
const columnDefinitions = [
  ["last", "Last", 2], ["percent_change", "% Chng", 2], ["mark", "Mark", 2], ["bid", "Bid", 2], ["ask", "Ask", 2], ["net_change", "Net Chng", 2],
  ["breakeven_distance", "BE Dist", 1], ["em_multiple", "BE / EM", 2], ["probability_beyond_breakeven", "Beyond BE", 1],
  ["probability_beyond_short", "Beyond Short", 1], ["credit_to_risk", "Credit / Risk", 1], ["market_width", "Mkt Width", 2],
  ["spread_delta", "Model Δ", 4], ["credit_position_gamma", "Credit Γ", 5], ["theta", "Leg Θ", 4], ["iv", "Short IV", 2],
  ["volume", "Volume", 0], ["open_interest", "Open Int", 0],
];
const defaultColumns = ["last", "mark", "bid", "ask", "breakeven_distance", "em_multiple", "probability_beyond_breakeven", "credit_to_risk", "spread_delta", "credit_position_gamma", "iv"];
const minimumStrikesEachSide = 25;
let visibleColumns = loadColumns();
let opportunityFilters = loadFilters();
let state = null;
let selectedShort = null;
let selectedLong = null;
let selectedAction = "OPEN";
let selectedMetrics = null;
let selectedOpeningCredit = null;
let livePreviewToken = null;
let submissionPending = false;
let lastHeartbeat = performance.now();
let lastStateReceived = performance.now();
let centeredExpiration = null;
let chainHorizontalInitialized = false;
const chainSnapTimers = new WeakMap();
const workingOrderDrafts = new Map();
let chainResizeTimer = null;
let chainViewportWidth = window.innerWidth;
const OPTION_COMMISSION_PER_LEG_CONTRACT = 0.65;
const ESTIMATED_OTHER_FEE_PER_LEG_CONTRACT = 0.012;

const money = (value) => value == null || !Number.isFinite(Number(value)) ? "—" : new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 }).format(Number(value));
const moneyExact = (value) => value == null || !Number.isFinite(Number(value)) ? "—" : new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(Number(value));
const number = (value, digits = 2) => value == null || !Number.isFinite(Number(value)) ? "—" : Number(value).toLocaleString("en-US", { minimumFractionDigits: digits, maximumFractionDigits: digits });
const age = (ms) => ms == null ? "—" : ms < 1000 ? `${Math.round(ms)} ms` : `${(ms / 1000).toFixed(1)} s`;
const easternTimestamp = (timestampMs) => timestampMs == null || !Number.isFinite(Number(timestampMs)) ? "—" : new Intl.DateTimeFormat("en-US", {
  timeZone: "America/New_York", weekday: "short", month: "short", day: "numeric",
  hour: "numeric", minute: "2-digit", second: "2-digit", timeZoneName: "short",
}).format(new Date(Number(timestampMs)));
const percent = (value, digits = 1) => value == null || !Number.isFinite(Number(value)) ? "—" : `${(Number(value) * 100).toFixed(digits)}%`;

function loadColumns() {
  try {
    const saved = JSON.parse(localStorage.getItem("ndxp-chain-columns-v4") || "null");
    const migrated = Array.isArray(saved) ? saved.map((key) => key === "delta" ? "spread_delta" : key === "gamma" ? "credit_position_gamma" : key) : [];
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
  renderTransmissionControl();
  if (state.mode === "DEMO") {
    ui["mode-banner"].textContent = "SYNTHETIC DEMO DATA · NOT CONNECTED TO DATABENTO OR SCHWAB MARKET DATA";
    ui["mode-banner"].className = "mode-banner";
  } else {
    ui["mode-banner"].className = "mode-banner hidden";
  }
  ui.spot.textContent = number(market.spot, 2);
  ui["quote-age"].textContent = market.spot_quote_time_ms == null ? "timestamp unavailable" : easternTimestamp(market.spot_quote_time_ms);
  ui["quote-age-label"].firstChild.textContent = `${market.spot_source || "NDX value"} · `;
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
  ui.diagnostic.textContent = diagnostics.classification === "MARKET_CLOSED_LATEST_QUOTES"
    ? "MARKET CLOSED · LATEST QUOTES"
    : diagnostics.classification || "STARTING";
  ui.diagnostic.className = diagnostics.classification === "HEALTHY" ? "status-good" : diagnostics.classification?.includes("ERROR") || diagnostics.classification?.includes("STALL") ? "status-bad" : "status-warn";
  renderExpirations(market.expirations || [], market.selected_expiration);
  renderAccounts(broker.accounts || [], broker.selected_account_hash);
  renderAnalytics(market.analytics || {});
  renderChain(market.selected_chain || {}, market.spot, market.analytics || {});
  renderPositions(broker.positions || []);
  renderOrders(broker.working_orders || [], broker.recent_orders || []);
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
  const liveTrading = Boolean(state?.transmission?.effective_enabled);
  ui.account.value = liveTrading ? selected : accounts.some((row) => row.hash === current) ? current : selected || accounts[0]?.hash || "";
  ui.account.disabled = liveTrading;
}

function renderTransmissionControl() {
  const enabled = Boolean(state?.transmission?.effective_enabled);
  ui["ticket-mode"].textContent = enabled ? "ORDER ENTRY · LIVE TRADING" : "ORDER ENTRY · PREVIEW ONLY";
  ui.submit.disabled = !enabled || !livePreviewToken || submissionPending;
  ui.submit.textContent = enabled ? "Transmit order to Schwab" : "Transmit to Schwab — locked";
}

function renderAnalytics(analytics) {
  const valid = analytics.status === "VALID";
  const rangeStrip = document.querySelector(".range-strip");
  rangeStrip.classList.toggle("invalid", !valid);
  rangeStrip.classList.toggle("snapshot", valid && analytics.quote_mode === "CLOSED_SNAPSHOT");
  if (!valid) {
    ui["expected-range"].textContent = "Analytics unavailable";
    ui["expected-move"].textContent = analytics.reason || "Waiting for independently valid option mids.";
    ui["derived-atm-iv"].textContent = "—";
    ui["time-remaining"].textContent = "—";
    ui["secondary-ranges"].textContent = "—";
    ui["candidate-count"].textContent = "—";
    ui["gamma-summary"].textContent = "—";
    ui["gamma-summary"].title = analytics.reason || "Gamma analytics unavailable.";
    return;
  }
  const one = analytics.ranges?.["1.0"];
  const half = analytics.ranges?.["0.5"];
  const oneHalf = analytics.ranges?.["1.5"];
  ui["expected-range"].textContent = one ? `${number(one.lower, 0)} – ${number(one.upper, 0)}` : "—";
  const snapshot = analytics.quote_mode === "CLOSED_SNAPSHOT";
  ui["expected-move"].textContent = snapshot
    ? `CLOSED SNAPSHOT · ${easternTimestamp(analytics.model_input_time_ms)} · ±${number(analytics.expected_move, 1)} points`
    : `±${number(analytics.expected_move, 1)} points · independently derived from option mids`;
  ui["derived-atm-iv"].textContent = `${number(analytics.atm_iv_percent, 2)}%`;
  ui["time-remaining"].textContent = formatDuration(analytics.seconds_remaining);
  ui["secondary-ranges"].textContent = half && oneHalf ? `0.5σ ${number(half.lower, 0)}–${number(half.upper, 0)} · 1.5σ ${number(oneHalf.lower, 0)}–${number(oneHalf.upper, 0)}` : "—";
  const modeledSpreads = Object.values(analytics.spreads || {});
  ui["gamma-summary"].textContent = `${modeledSpreads.length} spreads ready`;
  ui["gamma-summary"].title = `Independent credit-position gamma, nearest gamma flip, and −25/−10/+10/+25-point scenarios are available in each spread ticket${snapshot ? " from the labeled closed snapshot" : ""}.`;
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
    const call = callShort && callLong ? { short: callShort, long: callLong, metrics: spreadMetrics(callShort, callLong, analytics?.spreads?.[callShort.symbol]) } : null;
    const put = putShort && putLong ? { short: putShort, long: putLong, metrics: spreadMetrics(putShort, putLong, analytics?.spreads?.[putShort.symbol]) } : null;
    if (!call && !put) return [];
    return [{
      low, high,
      call, put,
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
    spread_delta: null, credit_position_delta: null, credit_position_gamma: null, leg_derived_credit_delta: difference(long, short, "delta"),
    theta: difference(long, short, "theta"), gamma: difference(long, short, "gamma"),
    iv: short.iv, volume: short.volume, open_interest: short.open_interest,
    ...(model || {}),
  };
}

function sideGridTemplate() {
  const widths = {
    breakeven_distance: 86, em_multiple: 86, probability_beyond_breakeven: 98,
    probability_beyond_short: 98, credit_to_risk: 92, market_width: 88,
    spread_delta: 86, credit_position_gamma: 92, open_interest: 94,
  };
  return visibleColumns.map((key) => `${widths[key] || 78}px`).join(" ");
}

function renderChain(chain, spot, analytics = {}) {
  const rows = verticalRows(chain, analytics);
  const positions = activePositionIndex();
  renderChainCoverage(chain, spot);
  const sideTemplate = sideGridTemplate();
  ui["call-header-table"].style.gridTemplateColumns = sideTemplate;
  ui["put-header-table"].style.gridTemplateColumns = sideTemplate;
  ui["strike-header-table"].style.gridTemplateColumns = "100%";
  ui["call-table"].style.gridTemplateColumns = sideTemplate;
  ui["put-table"].style.gridTemplateColumns = sideTemplate;
  ui["strike-table"].style.gridTemplateColumns = "100%";
  const callHeaderFragments = [];
  const strikeHeaderFragments = [];
  const putHeaderFragments = [];
  const callFragments = [], strikeFragments = [], putFragments = [];
  const callGroup = cell("CALLS", "group-head calls-head"); callGroup.style.gridColumn = `span ${visibleColumns.length}`;
  const strikeGroup = cell("10-POINT", "group-head strike-head");
  const putGroup = cell("PUTS", "group-head puts-head"); putGroup.style.gridColumn = `span ${visibleColumns.length}`;
  callHeaderFragments.push(callGroup);
  strikeHeaderFragments.push(strikeGroup);
  putHeaderFragments.push(putGroup);
  for (const key of visibleColumns) {
    const [, label] = columnDefinitions.find(([candidate]) => candidate === key);
    const callHeader = cell(label, "column-head");
    const putHeader = cell(label, "column-head");
    if (key === "spread_delta") {
      callHeader.title = "Independent model delta for the displayed long call vertical; selling it reverses the sign.";
      putHeader.title = "Independent model delta for the displayed long put vertical; selling it reverses the sign.";
    }
    if (key === "credit_position_gamma") {
      callHeader.title = "Independent Black-76 gamma for the short-credit position: long leg gamma minus short leg gamma.";
      putHeader.title = callHeader.title;
    }
    callHeaderFragments.push(callHeader);
    putHeaderFragments.push(putHeader);
  }
  strikeHeaderFragments.push(cell("Strikes", "column-head strike-cell"));
  for (const row of rows) {
    const near = Number.isFinite(Number(spot)) && row.low <= spot && row.high >= spot;
    const callOtm = row.call && Number(row.call.short.strike) > Number(spot);
    const putOtm = row.put && Number(row.put.short.strike) < Number(spot);
    const callItm = row.call && Number(row.call.short.strike) < Number(spot);
    const putItm = row.put && Number(row.put.short.strike) > Number(spot);
    for (const key of visibleColumns) callFragments.push(metricCell(row.call, key, near, "CALL", callOtm, callItm));
    strikeFragments.push(positionStrikeCell(row, near, positions));
    for (const key of visibleColumns) putFragments.push(metricCell(row.put, key, near, "PUT", putOtm, putItm));
  }
  if (!rows.length) {
    const callEmpty = cell("Waiting for calls…", "chain-empty");
    const putEmpty = cell("Waiting for puts…", "chain-empty");
    callEmpty.style.gridColumn = "1 / -1";
    putEmpty.style.gridColumn = "1 / -1";
    callFragments.push(callEmpty);
    strikeFragments.push(cell("—", "chain-empty strike-cell"));
    putFragments.push(putEmpty);
  }
  ui["call-header-table"].replaceChildren(...callHeaderFragments);
  ui["strike-header-table"].replaceChildren(...strikeHeaderFragments);
  ui["put-header-table"].replaceChildren(...putHeaderFragments);
  ui["call-table"].replaceChildren(...callFragments);
  ui["strike-table"].replaceChildren(...strikeFragments);
  ui["put-table"].replaceChildren(...putFragments);
  synchronizeChainHeader("call");
  synchronizeChainHeader("put");
  renderCandidateCount(rows, analytics);
  const expiration = state?.market?.selected_expiration || "";
  if (rows.length && expiration !== centeredExpiration) {
    centeredExpiration = expiration;
    requestAnimationFrame(() => ui["strike-table"].querySelector(".spread-strikes.near")?.scrollIntoView({ block: "center", inline: "nearest" }));
  }
  if (!chainHorizontalInitialized) {
    requestAnimationFrame(() => { anchorChainPanes(); chainHorizontalInitialized = true; });
  }
}

function activePositionIndex() {
  const broker = state?.broker || {};
  const selectedAccount = broker.selected_account_hash;
  return new Map((broker.positions || [])
    .filter((row) => !selectedAccount || row.account_hash === selectedAccount)
    .map((row) => [String(row.symbol || "").trim().toUpperCase(), row]));
}

function positionStrikeCell(row, near, positions) {
  const node = cell(`${number(row.low, 0)} / ${number(row.high, 0)}`, `strike-cell spread-strikes${near ? " near" : ""}`);
  appendPositionFlag(node, row.call?.short, "short", "call", "low", positions);
  appendPositionFlag(node, row.call?.long, "long", "call", "high", positions);
  appendPositionFlag(node, row.put?.long, "long", "put", "low", positions);
  appendPositionFlag(node, row.put?.short, "short", "put", "high", positions);
  return node;
}

function appendPositionFlag(node, contract, direction, side, level, positions) {
  const position = positions.get(String(contract?.symbol || "").trim().toUpperCase());
  const quantity = Number(position?.[`${direction}_quantity`] || 0);
  if (!(quantity > 0)) return;
  const marker = document.createElement("span");
  marker.className = `position-flag ${direction} ${side} ${level}`;
  marker.textContent = "⚑";
  marker.title = `${direction === "long" ? "Long" : "Short"} ${number(quantity, 0)} · ${contract.symbol}`;
  marker.setAttribute("aria-label", marker.title);
  node.append(marker);
}

function synchronizeChainHeader(side) {
  ui[`${side}-header-scroll`].scrollLeft = ui[`${side}-scroll`].scrollLeft;
}

function snapChainPane(side) {
  const pane = ui[`${side}-scroll`];
  const headerTable = ui[`${side}-header-table`];
  const headerCells = [...headerTable.querySelectorAll(".column-head")];
  const tableLeft = headerTable.getBoundingClientRect().left;
  const maximum = Math.max(0, pane.scrollWidth - pane.clientWidth);
  const targets = headerCells.map((header) => {
    const headerRect = header.getBoundingClientRect();
    const contentLeft = headerRect.left - tableLeft;
    const raw = side === "call"
      ? contentLeft + headerRect.width - pane.clientWidth
      : contentLeft;
    return Math.max(0, Math.min(maximum, raw));
  });
  if (!targets.length) return;
  const nearest = targets.reduce((best, candidate) =>
    Math.abs(candidate - pane.scrollLeft) < Math.abs(best - pane.scrollLeft) ? candidate : best
  );
  if (Math.abs(nearest - pane.scrollLeft) > 0.5) pane.scrollLeft = nearest;
  synchronizeChainHeader(side);
}

function handleChainScroll(side) {
  const pane = ui[`${side}-scroll`];
  synchronizeChainHeader(side);
  clearTimeout(chainSnapTimers.get(pane));
  chainSnapTimers.set(pane, setTimeout(() => snapChainPane(side), 110));
}

function anchorChainPanes() {
  ui["call-scroll"].scrollLeft = Math.max(0, ui["call-scroll"].scrollWidth - ui["call-scroll"].clientWidth);
  ui["put-scroll"].scrollLeft = 0;
  synchronizeChainHeader("call");
  synchronizeChainHeader("put");
}

function handleViewportGeometryChange(forceAnchor = false) {
  updateStickyHeaderOffset();
  const nextWidth = window.innerWidth;
  const materiallyChanged = Math.abs(nextWidth - chainViewportWidth) > 80;
  chainViewportWidth = nextWidth;
  if (!forceAnchor && !materiallyChanged) return;
  clearTimeout(chainResizeTimer);
  chainResizeTimer = setTimeout(() => requestAnimationFrame(anchorChainPanes), 180);
}

function updateStickyHeaderOffset() {
  const height = document.querySelector(".topbar")?.getBoundingClientRect().height || 92;
  document.documentElement.style.setProperty("--topbar-height", `${Math.ceil(height)}px`);
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

function metricCell(spread, key, near, side, otm = false, itm = false) {
  const definition = columnDefinitions.find(([candidate]) => candidate === key);
  const value = spread?.metrics?.[key];
  const formatted = formatMetric(key, value, definition[2]);
  const opportunity = opportunityState(spread?.metrics);
  if ((key === "bid" || key === "ask") && spread) {
    const button = document.createElement("button");
    const opening = key === "bid";
    button.className = `metric ${opening ? "bid-action" : "ask-action"}${near ? " near" : ""}${otm ? " otm" : ""}${itm ? " itm" : ""}${opening && opportunity.qualified ? " qualified" : ""}${opening && opportunity.preferred ? " preferred" : ""}${opening && spread.metrics?.credit_band === "ELEVATED" ? " elevated" : ""}`;
    button.textContent = formatted;
    const moneyness = otm ? " · OTM" : "";
    const midpoint = Number(spread.metrics?.mark);
    const positiveMidpoint = Number.isFinite(midpoint) && midpoint > 0;
    const naturalBidWarning = opening && Number(value) <= 0
      ? ` · natural bid ${number(value)} is non-positive; midpoint ticket ${number(midpoint)}`
      : "";
    button.title = opening
      ? `Sell to open ${side.toLowerCase()} credit spread${moneyness} · ${opportunity.label}${naturalBidWarning}`
      : `Buy to close ${side.toLowerCase()} credit spread${moneyness}`;
    button.disabled = opening ? !positiveMidpoint : value == null || Number(value) <= 0;
    button.addEventListener("click", () => openTicket(opening ? "OPEN" : "CLOSE", side, spread));
    button.setAttribute("role", "cell");
    return button;
  }
  return cell(formatted, `metric${near ? " near" : ""}${otm ? " otm" : ""}${itm ? " itm" : ""}${opportunity.qualified ? " qualified" : ""}`);
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

function openTicket(action, side, spread, requestedQuantity = null) {
  livePreviewToken = null;
  selectedAction = action;
  selectedMetrics = spread.metrics;
  selectedShort = spread.short; selectedLong = spread.long;
  const opening = action === "OPEN";
  selectedOpeningCredit = opening ? null : heldSpreadOpeningCredit(spread);
  ui["ticket-dialog"].classList.toggle("order-sell", opening);
  ui["ticket-dialog"].classList.toggle("order-buy", !opening);
  ui["ticket-side"].textContent = `${side} · ${opening ? "SELL TO OPEN" : "BUY TO CLOSE"}`;
  ui["ticket-title"].textContent = `${opening ? "Sell" : "Buy"} ${side.toLowerCase()} vertical`;
  ui["ticket-market"].textContent = `Bid ${number(spread.metrics.bid)} · Mid ${number(spread.metrics.mark)} · Ask ${number(spread.metrics.ask)}`;
  ui["short-instruction"].textContent = opening ? "SELL TO OPEN" : "BUY TO CLOSE";
  ui["long-instruction"].textContent = opening ? "BUY TO OPEN" : "SELL TO CLOSE";
  ui["short-leg"].textContent = `${selectedShort.symbol} · ${number(selectedShort.strike, 0)}`;
  ui["long-leg"].textContent = `${selectedLong.symbol} · ${number(selectedLong.strike, 0)}`;
  ui["limit-price-label"].textContent = opening ? "Limit credit" : "Limit debit";
  const heldQuantity = action === "CLOSE" ? heldSpreadQuantity(spread) : 0;
  ui.quantity.value = String(requestedQuantity || heldQuantity || 20);
  ui.credit.value = Math.max(0.05, Number(spread.metrics.mark || 0)).toFixed(2);
  ui["preview-result"].textContent = `${opening ? "Opening credit" : "Closing debit"} ticket constructed at the displayed mid. Review before building the Schwab payload.`;
  calculateRisk();
  renderTransmissionControl();
  ui["ticket-dialog"].showModal();
}

function heldSpreadQuantity(spread) {
  const positions = activePositionIndex();
  const shortPosition = positions.get(String(spread?.short?.symbol || "").trim().toUpperCase());
  const longPosition = positions.get(String(spread?.long?.symbol || "").trim().toUpperCase());
  return Math.min(
    Number(shortPosition?.short_quantity || 0),
    Number(longPosition?.long_quantity || 0),
  );
}

function heldSpreadOpeningCredit(spread) {
  const positions = activePositionIndex();
  const shortPosition = positions.get(String(spread?.short?.symbol || "").trim().toUpperCase());
  const longPosition = positions.get(String(spread?.long?.symbol || "").trim().toUpperCase());
  const shortAverage = Number(shortPosition?.average_price);
  const longAverage = Number(longPosition?.average_price);
  return Number.isFinite(shortAverage) && Number.isFinite(longAverage) ? shortAverage - longAverage : null;
}

function adjustPriceInput(input, increment) {
  const current = Number(input.value || 0);
  input.value = Math.min(9.99, Math.max(0.01, current + Number(increment))).toFixed(2);
  input.dispatchEvent(new Event("input", { bubbles: true }));
}

function calculateRisk() {
  const quantity = Number(ui.quantity.value || 0), limitPrice = Number(ui.credit.value || 0), gross = quantity * 10 * 100;
  const opening = selectedAction === "OPEN";
  const legContracts = quantity * 2;
  const estimatedOrderCosts = legContracts * (OPTION_COMMISSION_PER_LEG_CONTRACT + ESTIMATED_OTHER_FEE_PER_LEG_CONTRACT);
  const grossOrderCash = quantity * limitPrice * 100;
  const orderNetCash = opening ? grossOrderCash - estimatedOrderCosts : grossOrderCash + estimatedOrderCosts;
  const openingCredit = opening ? limitPrice : selectedOpeningCredit;
  const grossOpeningCash = openingCredit == null ? null : quantity * openingCredit * 100;
  const estimatedOpeningCosts = openingCredit == null ? null : estimatedOrderCosts;
  const netOpeningCash = grossOpeningCash == null ? null : grossOpeningCash - estimatedOpeningCosts;
  const netOpeningCreditPerSpread = openingCredit == null || !(quantity > 0) ? null : openingCredit - estimatedOpeningCosts / (quantity * 100);
  const estimatedTradePnl = !opening && netOpeningCash != null ? netOpeningCash - orderNetCash : null;
  ui["opening-gross-row"].classList.toggle("hidden", opening);
  ui["opening-net-row"].classList.toggle("hidden", opening);
  ui["trade-pnl-row"].classList.toggle("hidden", opening);
  ui["opening-gross"].textContent = moneyExact(grossOpeningCash);
  ui["opening-net"].textContent = moneyExact(netOpeningCash);
  ui.premium.textContent = moneyExact(grossOrderCash);
  ui.premium.className = opening ? "cash-credit" : "cash-debit";
  ui["order-cash-label"].textContent = opening ? "Gross opening credit" : "Proposed closing debit";
  ui["order-cost-label"].textContent = opening ? "Est. opening costs" : "Est. closing costs";
  ui["order-net-label"].textContent = opening ? "Est. net opening credit" : "Est. total closing debit";
  ui["order-costs"].textContent = moneyExact(estimatedOrderCosts);
  ui["order-net"].textContent = moneyExact(orderNetCash);
  ui["order-net"].className = opening ? "cash-credit" : "cash-debit";
  ui["trade-pnl"].textContent = moneyExact(estimatedTradePnl);
  ui["trade-pnl"].className = estimatedTradePnl == null ? "cash-neutral" : estimatedTradePnl >= 0 ? "cash-credit" : "cash-debit";
  ui["gross-risk"].textContent = `${number(10, 0)} pts × ${number(quantity, 0)} = ${money(gross)}`;
  ui["max-loss-label"].textContent = selectedAction === "OPEN" ? "Est. maximum loss incl. costs" : "Position effect";
  ui["max-loss"].textContent = opening ? moneyExact(gross - grossOrderCash + estimatedOrderCosts) : "Releases defined risk";
  ui["max-loss"].className = opening ? "cash-debit" : "cash-credit";
  const spot = Number(state?.market?.spot);
  const optionType = selectedMetrics?.option_type;
  const shortStrike = Number(selectedShort?.strike);
  const direction = optionType === "PUT" ? "below" : "above";
  ui.distance.textContent = Number.isFinite(shortStrike) && Number.isFinite(spot) ? `${number(Math.abs(shortStrike - spot))} pts ${direction}` : "—";
  const breakeven = Number.isFinite(shortStrike) && Number.isFinite(Number(netOpeningCreditPerSpread))
    ? optionType === "CALL" ? shortStrike + Number(netOpeningCreditPerSpread) : shortStrike - Number(netOpeningCreditPerSpread)
    : null;
  const breakevenDistance = breakeven == null || !Number.isFinite(spot)
    ? null
    : optionType === "CALL" ? breakeven - spot : spot - breakeven;
  const shortIvMove = Number(selectedMetrics?.short_iv_expected_move);
  ui["ticket-breakeven"].textContent = number(breakeven, 2);
  ui["breakeven-distance"].textContent = breakevenDistance == null ? "—" : `${number(breakevenDistance, 2)} pts ${direction}`;
  ui["short-iv-move"].textContent = Number.isFinite(shortIvMove) ? `±${number(shortIvMove, 1)} pts · ${number(selectedMetrics?.short_iv_percent, 2)}% IV` : "—";
  ui["ticket-em-multiple"].textContent = breakevenDistance == null || !(shortIvMove > 0) ? "—" : `${number(breakevenDistance / shortIvMove, 2)}×`;
  ui["ticket-credit-risk"].textContent = netOpeningCreditPerSpread == null || !(netOpeningCreditPerSpread > 0 && netOpeningCreditPerSpread < 10) ? "—" : percent(netOpeningCreditPerSpread / (10 - netOpeningCreditPerSpread), 1);
  const creditPositionDelta = selectedMetrics?.credit_position_delta;
  const creditPositionGamma = selectedMetrics?.credit_position_gamma;
  const actionSign = selectedAction === "OPEN" ? 1 : -1;
  ui["spread-delta"].textContent = number(creditPositionDelta, 4);
  ui["position-delta"].textContent = creditPositionDelta == null ? "—" : number(Number(creditPositionDelta) * quantity * actionSign, 2);
  ui["spread-gamma"].textContent = number(creditPositionGamma, 5);
  ui["position-gamma"].textContent = creditPositionGamma == null ? "—" : number(Number(creditPositionGamma) * quantity * actionSign, 4);
  const gammaAvailable = creditPositionGamma != null;
  ui["gamma-flip"].textContent = !gammaAvailable
    ? "Analytics unavailable"
    : selectedMetrics?.gamma_flip_spot == null
    ? "None in modeled range"
    : `${number(selectedMetrics.gamma_flip_spot, 2)} (${Number(selectedMetrics.gamma_flip_distance) >= 0 ? "+" : ""}${number(selectedMetrics.gamma_flip_distance, 1)} pts)`;
  const adverseMove = optionType === "PUT" ? -25 : 25;
  const adverseScenario = selectedMetrics?.gamma_scenarios?.find((row) => Number(row.spot_move) === adverseMove);
  ui["adverse-delta-label"].textContent = `Order Δ if NDX moves ${adverseMove > 0 ? "+" : ""}${adverseMove}`;
  ui["adverse-gamma-label"].textContent = `Order Γ if NDX moves ${adverseMove > 0 ? "+" : ""}${adverseMove}`;
  ui["adverse-delta"].textContent = adverseScenario ? number(Number(adverseScenario.credit_position_delta) * quantity * actionSign, 2) : "—";
  ui["gamma-scenario"].textContent = adverseScenario ? number(Number(adverseScenario.credit_position_gamma) * quantity * actionSign, 4) : "—";
  const openPositionGamma = creditPositionGamma == null ? null : Number(creditPositionGamma) * quantity;
  const adversePositionGamma = adverseScenario ? Number(adverseScenario.credit_position_gamma) * quantity : null;
  const gammaMultiple = openPositionGamma && adversePositionGamma != null ? Math.abs(adversePositionGamma / openPositionGamma) : null;
  const orderGammaChange = openPositionGamma == null ? null : openPositionGamma * actionSign;
  ui["gamma-alert"].className = `gamma-alert ${orderGammaChange == null ? "" : orderGammaChange < 0 ? "negative" : "positive"}`.trim();
  ui["gamma-alert"].textContent = openPositionGamma == null
    ? "Gamma analytics unavailable"
    : opening
    ? `${openPositionGamma < 0 ? "ADDS NEGATIVE Γ" : "ADDS POSITIVE Γ"} · adverse ${Math.abs(adverseMove)}-pt move ${gammaMultiple == null ? "—" : `${number(gammaMultiple, 2)}× current magnitude`}`
    : `${openPositionGamma < 0 ? "REMOVES NEGATIVE Γ" : "REMOVES POSITIVE Γ"} · held position ${number(openPositionGamma, 4)}`;
}

function orderPayload() {
  return { account_hash: ui.account.value, short_symbol: selectedShort?.symbol || "", long_symbol: selectedLong?.symbol || "", quantity: Number(ui.quantity.value), limit_price: ui.credit.value, action: selectedAction, duration: "DAY", session: "NORMAL" };
}

function invalidateLivePreview() {
  livePreviewToken = null;
  renderTransmissionControl();
}

async function preview() {
  if (!selectedShort || !selectedLong) return showNotice("Tap a call or put bid first.", true);
  try {
    const result = await api("/api/preview", { method: "POST", body: JSON.stringify(orderPayload()) });
    livePreviewToken = result.preview_token || null;
    ui["preview-result"].textContent = JSON.stringify(result, null, 2);
    renderTransmissionControl();
    showNotice(livePreviewToken ? "Live-order preview built. The token is single-use for 60 seconds." : "Order preview built. No broker mutation was attempted.");
  } catch (error) { showNotice(error.message, true); }
}

async function submitLiveOrder() {
  if (!livePreviewToken || submissionPending) return showNotice("Build a fresh live-order preview first.", true);
  const payload = { ...orderPayload(), preview_token: livePreviewToken };
  const priceEffect = payload.action === "OPEN" ? "credit" : "debit";
  const summary = `${payload.action} ${payload.quantity} ${selectedShort?.symbol} / ${selectedLong?.symbol} at ${payload.limit_price} ${priceEffect}`;
  if (!window.confirm(`Transmit this Schwab order?\n\n${summary}\n\nThe preview token can be used only once.`)) return;
  submissionPending = true;
  renderTransmissionControl();
  const token = livePreviewToken;
  livePreviewToken = null;
  try {
    const result = await api("/api/submit", { method: "POST", body: JSON.stringify({ ...payload, preview_token: token }) });
    ui["preview-result"].textContent = JSON.stringify(result, null, 2);
    showNotice(`Schwab accepted the order request${result.broker_order_id ? ` as order ${result.broker_order_id}` : ""}.`);
  } catch (error) {
    showNotice(`${error.message} Do not resubmit until Schwab working orders are reconciled.`, true);
  } finally {
    submissionPending = false;
    renderTransmissionControl();
  }
}

function renderPositions(rows) {
  if (!rows.length) { ui.positions.className = "empty"; ui.positions.textContent = "No NDX/NDXP positions in current Schwab account truth."; return; }
  ui.positions.className = "";
  const positions = activePositionIndex();
  const matchedSymbols = new Set();
  const cards = [];
  for (const row of verticalRows(state?.market?.selected_chain || {}, state?.market?.analytics || {})) {
    for (const [side, spread] of [["CALL", row.call], ["PUT", row.put]]) {
      if (!spread) continue;
      const quantity = heldSpreadQuantity(spread);
      if (!(quantity > 0)) continue;
      const shortSymbol = String(spread.short.symbol || "").trim().toUpperCase();
      const longSymbol = String(spread.long.symbol || "").trim().toUpperCase();
      const shortPosition = positions.get(shortSymbol);
      const longPosition = positions.get(longSymbol);
      matchedSymbols.add(shortSymbol); matchedSymbols.add(longSymbol);
      const averageCredit = Number(shortPosition?.average_price) - Number(longPosition?.average_price);
      const node = document.createElement("div"); node.className = "position-card";
      const head = document.createElement("div"); head.className = "order-card-head";
      const title = document.createElement("strong");
      title.textContent = `${side} ${number(spread.short.strike, 0)} / ${number(spread.long.strike, 0)} · ${number(quantity, 0)} spreads`;
      const mark = document.createElement("span"); mark.className = "order-status"; mark.textContent = `MARK ${number(spread.metrics.mark)}`;
      head.append(title, mark);
      const detail = document.createElement("small"); detail.className = "order-detail";
      const positionDelta = spread.metrics.credit_position_delta == null ? null : Number(spread.metrics.credit_position_delta) * quantity;
      const positionGamma = spread.metrics.credit_position_gamma == null ? null : Number(spread.metrics.credit_position_gamma) * quantity;
      const gammaFlip = spread.metrics.gamma_flip_spot == null ? "Γ flip not found in modeled range" : `Γ flip ${number(spread.metrics.gamma_flip_spot, 2)}`;
      detail.textContent = `Average opening credit ${Number.isFinite(averageCredit) ? number(averageCredit) : "—"} · Position Δ ${number(positionDelta, 2)} · Position Γ ${number(positionGamma, 4)} · ${gammaFlip} · ${spread.short.symbol} / ${spread.long.symbol}`;
      const actions = document.createElement("div"); actions.className = "actions";
      const close = document.createElement("button"); close.textContent = "Close at current mid";
      close.addEventListener("click", () => openTicket("CLOSE", side, spread, quantity));
      actions.append(close); node.append(head, detail, actions); cards.push(node);
    }
  }
  for (const row of rows) {
    if (!matchedSymbols.has(String(row.symbol || "").trim().toUpperCase())) {
      cards.push(dataRow(row.symbol, `${row.long_quantity || 0} long · ${row.short_quantity || 0} short · avg ${row.average_price ?? "—"}`));
    }
  }
  ui.positions.replaceChildren(...cards);
}

function renderOrders(rows, recentRows = []) {
  if (document.activeElement?.matches("#orders input")) return;
  if (!rows.length && !recentRows.length) { workingOrderDrafts.clear(); ui.orders.className = "empty"; ui.orders.textContent = "No recent NDX/NDXP orders."; return; }
  ui.orders.className = "";
  const activeIds = new Set(rows.map((row) => row.order_id));
  for (const orderId of workingOrderDrafts.keys()) if (!activeIds.has(orderId)) workingOrderDrafts.delete(orderId);
  const activity = recentRows.filter((row) => !activeIds.has(row.order_id));
  ui.orders.replaceChildren(...rows.map(workingOrderCard), ...activity.map(recentOrderCard));
}

function recentOrderCard(row) {
  const node = document.createElement("div"); node.className = "working-order-card recent-order-card";
  const head = document.createElement("div"); head.className = "order-card-head";
  const title = document.createElement("strong"); title.textContent = `#${row.order_id} · ${row.action || row.order_type || "ORDER"}`;
  const status = document.createElement("span");
  const statusText = String(row.status || "UNKNOWN").toUpperCase();
  status.className = `order-status order-status-${statusText.toLowerCase().replaceAll("_", "-")}`;
  status.textContent = statusText;
  head.append(title, status);
  const detail = document.createElement("small"); detail.className = "order-detail";
  detail.textContent = row.legs.map((leg) => `${leg.instruction} ${leg.quantity} ${leg.symbol}`).join(" · ");
  const timing = document.createElement("small"); timing.className = "order-detail";
  const timestamp = row.close_time || row.entered_time;
  const fill = row.filled_quantity != null ? ` · filled ${number(row.filled_quantity, 0)}` : "";
  timing.textContent = `${timestamp ? new Date(timestamp).toLocaleString() : "Time unavailable"}${fill}`;
  node.append(head, detail, timing);
  return node;
}

function workingOrderCard(row) {
  const draft = workingOrderDrafts.get(row.order_id) || {};
  const node = document.createElement("div"); node.className = "working-order-card";
  const head = document.createElement("div"); head.className = "order-card-head";
  const title = document.createElement("strong"); title.textContent = `#${row.order_id} · ${row.action || row.order_type || "ORDER"}`;
  const status = document.createElement("span"); status.className = "order-status"; status.textContent = row.status || "WORKING";
  head.append(title, status);
  const detail = document.createElement("small"); detail.className = "order-detail";
  detail.textContent = row.legs.map((leg) => `${leg.instruction} ${leg.quantity} ${leg.symbol}`).join(" · ");
  const entered = document.createElement("small"); entered.className = "order-detail";
  entered.textContent = row.entered_time ? `Entered ${new Date(row.entered_time).toLocaleString()}` : "";
  const fields = document.createElement("div"); fields.className = "inline-order-fields";
  const quantityLabel = document.createElement("label"); quantityLabel.textContent = "Quantity";
  const quantity = document.createElement("input"); quantity.type = "number"; quantity.min = "1"; quantity.max = "100"; quantity.value = draft.quantity ?? row.quantity ?? "";
  quantityLabel.append(quantity);
  const priceLabel = document.createElement("label"); priceLabel.textContent = row.action === "CLOSE" ? "Net debit" : "Net credit";
  const price = document.createElement("input"); price.type = "number"; price.min = "0.01"; price.max = "9.99"; price.step = "0.05"; price.value = draft.price ?? Number(row.price || 0).toFixed(2);
  priceLabel.append(price);
  const preserveDraft = () => workingOrderDrafts.set(row.order_id, { quantity: quantity.value, price: price.value });
  quantity.addEventListener("input", preserveDraft); price.addEventListener("input", preserveDraft);
  const actions = document.createElement("div"); actions.className = "actions";
  const live = Boolean(state?.transmission?.effective_enabled);
  const update = document.createElement("button"); update.textContent = live ? "Update order" : "Update (locked)";
  update.disabled = !live || !row.editable;
  update.addEventListener("click", async () => {
    update.disabled = true;
    const result = await replaceWorkingOrder(row, quantity, price);
    if (!result) update.disabled = !live || !row.editable;
  });
  const cancel = document.createElement("button"); cancel.textContent = live ? "Cancel" : "Cancel (locked)";
  cancel.disabled = !live;
  cancel.addEventListener("click", async () => {
    cancel.disabled = true;
    workingOrderDrafts.delete(row.order_id);
    const result = await lockedAction("cancel", { account_hash: ui.account.value, broker_order_id: row.order_id });
    if (!result) cancel.disabled = !live;
  });
  actions.append(update, cancel); fields.append(quantityLabel, priceLabel, actions);
  const priceSteps = document.createElement("div"); priceSteps.className = "inline-price-steps";
  for (const step of [-0.25, -0.10, -0.05, 0.05, 0.10, 0.25]) {
    const button = document.createElement("button"); button.type = "button";
    button.textContent = `${step > 0 ? "+" : "−"}${Math.abs(step).toFixed(2)}`;
    button.addEventListener("click", () => { adjustPriceInput(price, step); preserveDraft(); });
    priceSteps.append(button);
  }
  node.append(head, detail, entered, fields, priceSteps);
  return node;
}

async function replaceWorkingOrder(row, quantityInput, priceInput) {
  const quantity = Number(quantityInput.value);
  const price = Number(priceInput.value);
  if (!row.editable || !Number.isInteger(quantity) || quantity < 1 || quantity > 100 || !(price > 0 && price < 10)) {
    showNotice("Enter a whole-number quantity from 1–100 and a valid net price below 10.00.", true);
    return null;
  }
  const result = await lockedAction("replace", {
    account_hash: ui.account.value,
    broker_order_id: row.order_id,
    short_symbol: row.short_symbol,
    long_symbol: row.long_symbol,
    quantity,
    limit_price: price.toFixed(2),
    action: row.action,
    duration: "DAY",
    session: "NORMAL",
  });
  if (result) workingOrderDrafts.delete(row.order_id);
  return result;
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
  try {
    const result = await api(`/api/${action}`, { method: "POST", body: JSON.stringify(payload) });
    const label = action === "cancel" ? "Cancellation" : action === "replace" ? "Order update" : action;
    showNotice(`${label} accepted by Schwab${result.broker_order_id ? ` for order ${result.broker_order_id}` : ""}.`);
    await refresh();
    return result;
  }
  catch (error) { showNotice(`${error.message} Reconcile the Schwab order list before trying again.`, true); return null; }
}

async function selectExpiration() {
  invalidateLivePreview();
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
      localStorage.setItem("ndxp-chain-columns-v4", JSON.stringify(visibleColumns));
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
ui.quantity.addEventListener("input", () => { invalidateLivePreview(); calculateRisk(); });
ui.credit.addEventListener("input", () => { invalidateLivePreview(); calculateRisk(); });
document.querySelectorAll("[data-ticket-price-step]").forEach((button) => button.addEventListener("click", () => {
  adjustPriceInput(ui.credit, Number(button.dataset.ticketPriceStep));
}));
ui.preview.addEventListener("click", preview); ui.submit.addEventListener("click", submitLiveOrder);
ui["columns-button"].addEventListener("click", () => ui["columns-dialog"].showModal());
ui["filters-button"].addEventListener("click", () => { populateFilters(); ui["filters-dialog"].showModal(); });
ui["save-filters"].addEventListener("click", (event) => {
  if (!saveFilters()) event.preventDefault();
});
document.querySelectorAll("[data-jump]").forEach((button) => button.addEventListener("click", () => document.getElementById(button.dataset.jump).scrollIntoView({ behavior: "smooth" })));
ui["call-scroll"].addEventListener("scroll", () => handleChainScroll("call"), { passive: true });
ui["put-scroll"].addEventListener("scroll", () => handleChainScroll("put"), { passive: true });
new ResizeObserver(updateStickyHeaderOffset).observe(document.querySelector(".topbar"));
window.addEventListener("resize", () => handleViewportGeometryChange(false), { passive: true });
window.addEventListener("orientationchange", () => handleViewportGeometryChange(true), { passive: true });
ui["access-check"].addEventListener("click", async () => {
  try { showNotice("Running read-only Schwab account and NDX chain checks…"); const result = await api("/api/access-check"); showNotice(`Access verified: ${JSON.stringify(result)}`); }
  catch (error) { showNotice(`Access check failed: ${error.message}`, true); }
});

renderColumnOptions();
updateStickyHeaderOffset();
refresh(); heartbeat(); setInterval(refresh, 1000); setInterval(heartbeat, 1000);
