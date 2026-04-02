const PAPER_FILL_ALERT_STORAGE_KEY = "mgcDash.paperFillAlerts.v1";
const PAPER_FILL_ALERT_MAX_IDS = 400;
const PAPER_FILL_ALERT_STALE_WARMUP_MS = 90 * 1000;
let paperFillAudioContext = null;

const state = {
  dashboard: null,
  refreshIntervalSeconds: 15,
  refreshTimer: null,
  refreshInFlight: false,
  actionInFlight: false,
  lastAction: null,
  selectedApprovedModel: null,
  filters: {
    blotter: "",
    fills: "",
    intents: "",
    historicalPlayback: "",
    branchSession: "",
    branchHistory: "",
    recent: "",
    sessionHistory: "",
  },
  paperFillAlerts: loadPaperFillAlertState(),
};

async function fetchDashboard() {
  if (state.refreshInFlight) return;
  state.refreshInFlight = true;
  try {
    const response = await fetch("/api/dashboard");
    if (!response.ok) {
      throw new Error(`Dashboard fetch failed: ${response.status}`);
    }
    state.dashboard = await response.json();
    if (state.dashboard?.refresh?.default_interval_seconds && !Number.isFinite(state.refreshIntervalSeconds)) {
      state.refreshIntervalSeconds = state.dashboard.refresh.default_interval_seconds;
    }
    processPaperFillAlerts(state.dashboard?.paper?.latest_fills || []);
    render();
  } catch (error) {
    setActionOutput({
      action_label: "Refresh Status",
      kind: "failed",
      timestamp: new Date().toISOString(),
      message: String(error),
      command: null,
      stdout_snippet: "",
      stderr_snippet: String(error),
      output: String(error),
    });
    render();
  } finally {
    state.refreshInFlight = false;
    scheduleRefresh();
  }
}

function scheduleRefresh() {
  if (state.refreshTimer) {
    clearTimeout(state.refreshTimer);
    state.refreshTimer = null;
  }
  if (!state.refreshIntervalSeconds || state.refreshIntervalSeconds <= 0) return;
  state.refreshTimer = setTimeout(fetchDashboard, state.refreshIntervalSeconds * 1000);
}

function loadPaperFillAlertState() {
  const stored = readPaperFillAlertStorage();
  return {
    initialized: false,
    desktopEnabled: Boolean(stored.desktopEnabled),
    soundEnabled: stored.soundEnabled !== false,
    permissionRequested: Boolean(stored.permissionRequested),
    permission: notificationPermissionState(),
    seenFillIds: new Set(Array.isArray(stored.seenFillIds) ? stored.seenFillIds : []),
    lastAlertAt: stored.lastAlertAt || null,
    lastAlertSummary: stored.lastAlertSummary || "No new paper fill alert in this browser yet.",
  };
}

function readPaperFillAlertStorage() {
  try {
    const raw = window.localStorage.getItem(PAPER_FILL_ALERT_STORAGE_KEY);
    return raw ? JSON.parse(raw) : {};
  } catch (_error) {
    return {};
  }
}

function persistPaperFillAlertState() {
  const alerts = state.paperFillAlerts;
  if (!alerts) return;
  trimSeenPaperFillIds(alerts.seenFillIds);
  try {
    window.localStorage.setItem(
      PAPER_FILL_ALERT_STORAGE_KEY,
      JSON.stringify({
        desktopEnabled: alerts.desktopEnabled,
        soundEnabled: alerts.soundEnabled,
        permissionRequested: alerts.permissionRequested,
        seenFillIds: Array.from(alerts.seenFillIds),
        lastAlertAt: alerts.lastAlertAt,
        lastAlertSummary: alerts.lastAlertSummary,
      }),
    );
  } catch (_error) {
    // Ignore localStorage failures and keep alerts in-memory only.
  }
}

function notificationPermissionState() {
  if (typeof window === "undefined" || !("Notification" in window)) {
    return "unsupported";
  }
  return window.Notification.permission || "default";
}

function trimSeenPaperFillIds(seenFillIds) {
  while (seenFillIds.size > PAPER_FILL_ALERT_MAX_IDS) {
    const oldest = seenFillIds.values().next().value;
    if (!oldest) break;
    seenFillIds.delete(oldest);
  }
}

function processPaperFillAlerts(rows) {
  const alerts = state.paperFillAlerts;
  if (!alerts) return;
  alerts.permission = notificationPermissionState();

  const fillEntries = (Array.isArray(rows) ? rows : [])
    .map((row) => ({ row, id: persistedPaperFillIdentity(row) }))
    .filter((entry) => Boolean(entry.id));

  if (!alerts.initialized) {
    fillEntries.forEach((entry) => {
      alerts.seenFillIds.add(entry.id);
    });
    alerts.initialized = true;
    persistPaperFillAlertState();
    return;
  }

  const newEntries = fillEntries.filter((entry) => !alerts.seenFillIds.has(entry.id));
  if (!newEntries.length) {
    return;
  }

  newEntries.forEach((entry) => {
    alerts.seenFillIds.add(entry.id);
  });

  const burst = summarizePaperFillBurst(newEntries.map((entry) => entry.row));
  alerts.lastAlertAt = burst.timestamp;
  alerts.lastAlertSummary = burst.statusLine;
  persistPaperFillAlertState();
  showPaperFillToast(burst);
  maybeSendPaperFillDesktopNotification(burst);
  playPaperFillAlertBurst(newEntries.length);
}

function persistedPaperFillIdentity(row) {
  if (!row || typeof row !== "object") return null;
  if (row.fill_id != null && row.fill_id !== "") {
    return `fill:${row.fill_id}`;
  }
  const orderIntentId = row.order_intent_id || "unknown-intent";
  const fillTimestamp = row.fill_timestamp || "unknown-time";
  const fillPrice = row.fill_price || "unknown-price";
  const brokerOrderId = row.broker_order_id || "unknown-broker";
  const intentType = row.intent_type || "unknown-intent-type";
  return `fill:${orderIntentId}:${fillTimestamp}:${fillPrice}:${brokerOrderId}:${intentType}`;
}

function summarizePaperFillBurst(rows) {
  const normalizedRows = Array.isArray(rows) ? rows.filter(Boolean) : [];
  const count = normalizedRows.length;
  const latest = normalizedRows[0] || {};
  const latestTimestamp = latest.fill_timestamp || new Date().toISOString();
  const countLabel = count === 1 ? "1 new paper fill" : `${count} new paper fills`;
  if (count === 1) {
    const side = latest.intent_type || "FILL";
    const price = latest.fill_price || "-";
    const intent = latest.order_intent_id || latest.broker_order_id || "persisted fill";
    return {
      title: "Paper Fill Observed",
      body: `${side} @ ${price} • ${intent}`,
      statusLine: `${latestTimestamp} • ${side} @ ${price} • ${intent}`,
      timestamp: latestTimestamp,
    };
  }
  const intentTypes = normalizedRows
    .map((row) => row.intent_type)
    .filter(Boolean)
    .reduce((counts, value) => {
      counts[value] = (counts[value] || 0) + 1;
      return counts;
    }, {});
  const typeSummary = Object.entries(intentTypes)
    .map(([label, value]) => `${label} x${value}`)
    .join(", ");
  return {
    title: `${count} Paper Fills Observed`,
    body: typeSummary ? `${countLabel} • ${typeSummary}` : countLabel,
    statusLine: `${latestTimestamp} • ${countLabel}${typeSummary ? ` • ${typeSummary}` : ""}`,
    timestamp: latestTimestamp,
  };
}

function maybeSendPaperFillDesktopNotification(burst) {
  const alerts = state.paperFillAlerts;
  if (!alerts?.desktopEnabled || alerts.permission !== "granted") return;
  try {
    new window.Notification(burst.title, {
      body: burst.body,
      tag: "mgc-paper-fill-alert",
      renotify: true,
      silent: true,
    });
  } catch (_error) {
    // Ignore browser notification failures; toast/status line still render.
  }
}

function showPaperFillToast(burst) {
  const container = document.getElementById("paper-fill-toast-stack");
  if (!container) return;
  const toast = document.createElement("div");
  toast.className = "toast";
  toast.innerHTML = `
    <div class="toast-title">${escapeHtml(burst.title)}</div>
    <div class="toast-body">${escapeHtml(burst.body)}</div>
    <div class="toast-meta">${escapeHtml(burst.timestamp || "-")}</div>
  `;
  container.prepend(toast);
  while (container.children.length > 3) {
    container.removeChild(container.lastElementChild);
  }
  window.setTimeout(() => {
    toast.remove();
  }, 6500);
}

function warmPaperFillAudioContext() {
  const AudioContextCtor = window.AudioContext || window.webkitAudioContext;
  if (!AudioContextCtor) return;
  try {
    if (!paperFillAudioContext) {
      paperFillAudioContext = new AudioContextCtor();
    }
    if (paperFillAudioContext.state === "suspended") {
      paperFillAudioContext.resume().catch(() => {});
    }
  } catch (_error) {
    // Ignore browser audio warm-up failures.
  }
}

async function playPaperFillAlertBurst(fillCount) {
  const alerts = state.paperFillAlerts;
  if (!alerts?.soundEnabled) return;
  const AudioContextCtor = window.AudioContext || window.webkitAudioContext;
  if (!AudioContextCtor) return;
  try {
    if (!paperFillAudioContext) {
      paperFillAudioContext = new AudioContextCtor();
    }
    if (paperFillAudioContext.state === "suspended") {
      await paperFillAudioContext.resume();
    }
  } catch (_error) {
    return;
  }

  const toneOffsets = fillCount > 1 ? [0, 0.16] : [0];
  toneOffsets.forEach((offset, index) => {
    const oscillator = paperFillAudioContext.createOscillator();
    const gain = paperFillAudioContext.createGain();
    oscillator.type = "sine";
    oscillator.frequency.value = index === 0 ? 932 : 1108;
    gain.gain.setValueAtTime(0.0001, paperFillAudioContext.currentTime + offset);
    gain.gain.exponentialRampToValueAtTime(0.05, paperFillAudioContext.currentTime + offset + 0.01);
    gain.gain.exponentialRampToValueAtTime(0.0001, paperFillAudioContext.currentTime + offset + 0.18);
    oscillator.connect(gain);
    gain.connect(paperFillAudioContext.destination);
    oscillator.start(paperFillAudioContext.currentTime + offset);
    oscillator.stop(paperFillAudioContext.currentTime + offset + 0.2);
  });
}

async function requestPaperFillNotificationPermission(options = {}) {
  const alerts = state.paperFillAlerts;
  const enableOnGrant = Boolean(options.enableOnGrant);
  if (!alerts) return;
  if (notificationPermissionState() === "unsupported") {
    alerts.desktopEnabled = false;
    alerts.permission = "unsupported";
    persistPaperFillAlertState();
    renderPaperFillAlerts();
    return;
  }
  try {
    const permission = await window.Notification.requestPermission();
    alerts.permissionRequested = true;
    alerts.permission = permission;
    if (permission === "granted" && enableOnGrant) {
      alerts.desktopEnabled = true;
    } else if (permission !== "granted") {
      alerts.desktopEnabled = false;
    }
  } catch (_error) {
    alerts.desktopEnabled = false;
    alerts.permission = notificationPermissionState();
  }
  persistPaperFillAlertState();
  renderPaperFillAlerts();
}

async function togglePaperFillDesktopAlerts() {
  const alerts = state.paperFillAlerts;
  if (!alerts) return;
  if (alerts.desktopEnabled) {
    alerts.desktopEnabled = false;
    persistPaperFillAlertState();
    renderPaperFillAlerts();
    return;
  }
  if (notificationPermissionState() === "granted") {
    alerts.desktopEnabled = true;
    alerts.permission = "granted";
    persistPaperFillAlertState();
    renderPaperFillAlerts();
    return;
  }
  await requestPaperFillNotificationPermission({ enableOnGrant: true });
}

function togglePaperFillSoundAlerts() {
  const alerts = state.paperFillAlerts;
  if (!alerts) return;
  alerts.soundEnabled = !alerts.soundEnabled;
  persistPaperFillAlertState();
  renderPaperFillAlerts();
}

function renderPaperFillAlerts() {
  const alerts = state.paperFillAlerts;
  if (!alerts) return;
  alerts.permission = notificationPermissionState();

  const desktopState = alerts.desktopEnabled && alerts.permission === "granted"
    ? { label: "ON", level: "ok" }
    : alerts.permission === "unsupported"
      ? { label: "UNSUPPORTED", level: "muted" }
      : alerts.permission === "denied"
        ? { label: "BLOCKED", level: "danger" }
        : alerts.desktopEnabled
          ? { label: "PENDING", level: "warning" }
          : { label: "OFF", level: "muted" };
  setStatusValue("paper-fill-desktop-status", desktopState.label, desktopState.level);
  setStatusValue("paper-fill-sound-status", alerts.soundEnabled ? "ON" : "MUTED", alerts.soundEnabled ? "ok" : "warning");
  text("paper-fill-notification-permission", String(alerts.permission || "unsupported").toUpperCase());
  text("paper-fill-last-alert-status", alerts.lastAlertSummary || "No new paper fill alert in this browser yet.");

  const desktopToggle = document.getElementById("paper-fill-desktop-toggle");
  if (desktopToggle) {
    desktopToggle.textContent = alerts.desktopEnabled ? "Desktop Alerts On" : "Desktop Alerts Off";
    desktopToggle.disabled = alerts.permission === "unsupported";
    desktopToggle.title = alerts.permission === "unsupported"
      ? "Browser desktop notifications are unavailable here."
      : "Toggle browser notifications for newly observed paper fills only.";
  }

  const soundToggle = document.getElementById("paper-fill-sound-toggle");
  if (soundToggle) {
    soundToggle.textContent = alerts.soundEnabled ? "Sound On" : "Sound Muted";
    soundToggle.title = "Toggle sound for newly observed paper fills only.";
  }

  const requestButton = document.getElementById("paper-fill-notification-request");
  if (requestButton) {
    if (alerts.permission === "granted") {
      requestButton.classList.add("hidden");
      requestButton.disabled = true;
    } else {
      requestButton.classList.remove("hidden");
      requestButton.disabled = alerts.permission === "denied" || alerts.permission === "unsupported";
      requestButton.textContent = alerts.permission === "unsupported"
        ? "Desktop Unsupported"
        : alerts.permission === "denied"
          ? "Permission Denied"
          : "Request Permission";
    }
  }

  const noteParts = [
    "New alerts fire only for newly observed persisted paper fills.",
    "Signals, blocks, decisions, and intents never alert here.",
    alerts.permission === "granted"
      ? "Browser notifications are permitted."
      : alerts.permission === "default"
        ? "Desktop alerts need browser permission."
        : alerts.permission === "denied"
          ? "Desktop alerts are blocked by browser permission."
          : "Desktop notifications are unsupported in this browser.",
  ];
  text("paper-fill-alert-note", noteParts.join(" "));
}

async function runAction(action) {
  if (state.actionInFlight) return;
  if (action === "paper-flatten-and-halt" || action === "atp-companion-paper-flatten-and-halt") {
    const confirmed = window.confirm(
      "Confirm PAPER-ONLY Flatten And Halt?\n\nThis will halt new entries immediately and, if safe, submit a deterministic paper flatten request. Live routing remains disabled."
    );
    if (!confirmed) {
      return;
    }
  }
  if (action === "paper-stop-after-cycle" || action === "atp-companion-paper-stop-after-cycle") {
    const confirmed = window.confirm(
      "Confirm PAPER-ONLY Stop After Current Cycle?\n\nThis will halt new entries and stop the paper runtime at the next safe flat point. Live routing remains disabled."
    );
    if (!confirmed) {
      return;
    }
  }
  if (action === "paper-clear-risk-halts") {
    const confirmed = window.confirm(
      "Confirm PAPER-ONLY Clear Risk Halts?\n\nUse this only after inspection confirms the desk is flat, reconciled, and safe. You will still need Resume Entries to re-arm eligible lanes."
    );
    if (!confirmed) {
      return;
    }
  }
  if (action === "paper-force-reconcile") {
    const confirmed = window.confirm(
      "Confirm PAPER-ONLY Force Reconcile?\n\nThis reruns broker-vs-internal reconciliation immediately. Safe repairs may be applied automatically, but unresolved ambiguity will still keep entries frozen."
    );
    if (!confirmed) {
      return;
    }
  }
  if (action === "sign-off-paper-session") {
    const confirmed = window.confirm(
      "Confirm paper session sign-off?\n\nOnly proceed if the close checklist is complete and any remaining risk has been explicitly acknowledged."
    );
    if (!confirmed) {
      return;
    }
  }
  if (action === "resolve-inherited-risk") {
    const confirmed = window.confirm(
      "Confirm inherited prior-session risk resolution?\n\nOnly proceed if the previous session has actually been remediated and the carry-forward checklist is now truthfully clear."
    );
    if (!confirmed) {
      return;
    }
  }
  state.actionInFlight = true;
  setActionOutput({
    action_label: action,
    kind: "pending",
    timestamp: new Date().toISOString(),
    message: `Running ${action}...`,
    command: null,
    stdout_snippet: "",
    stderr_snippet: "",
    output: "",
  });
  render();
  try {
    const response = await fetch(`/api/action/${action}`, { method: "POST" });
    const payload = await response.json();
    state.lastAction = payload;
    if (payload.snapshot) {
      state.dashboard = payload.snapshot;
      processPaperFillAlerts(state.dashboard?.paper?.latest_fills || []);
    }
    setActionOutput(payload);
  } catch (error) {
    setActionOutput({
      action_label: action,
      kind: "failed",
      timestamp: new Date().toISOString(),
      message: String(error),
      command: null,
      stdout_snippet: "",
      stderr_snippet: String(error),
      output: String(error),
    });
  } finally {
    state.actionInFlight = false;
    render();
    scheduleRefresh();
  }
}

function render() {
  const dashboard = state.dashboard;
  if (!dashboard) return;
  const { global, market_context, treasury_curve, historical_playback, shadow, paper, manual_controls, active_decision_mode, review, action_log, paper_operator_state, paper_risk_state, paper_closeout, paper_session_close_review, paper_carry_forward, paper_pre_session_review, paper_run_start, paper_continuity } = dashboard;

  setBadge("badge-mode", global.mode_label, levelForMode(global.mode));
  setBadge("badge-auth", global.auth_label, global.auth_ready ? "ok" : "danger");
  setBadge("badge-desk-clean", global.desk_clean_label || "DESK UNKNOWN", global.desk_clean ? "ok" : "warning");
  setBadge("badge-paper-ready", global.paper_run_ready_label || "RUN READINESS UNKNOWN", global.paper_run_ready ? "ok" : "warning");
  text("mode-chip", global.mode_label || "-");
  setStatusValue("shadow-global-status", global.shadow_label, shadow.running ? "ok" : "muted");
  setStatusValue("paper-global-status", global.paper_label, paper.running ? "ok" : "muted");
  setStatusValue("market-data-status", global.market_data_label, levelForMarketData(global.market_data_label));
  setStatusValue("runtime-health", global.runtime_health_label, levelForHealth(global.runtime_health_label));
  setStatusValue("reconciliation-status", global.reconciliation_status, global.reconciliation_status === "CLEAN" ? "ok" : "danger");
  setStatusValue("fault-state", global.fault_state, global.fault_state === "FAULTED" ? "danger" : "ok");
  text("desk-freshness", global.stale ? `STALE ${Math.round(global.artifact_age_seconds || 0)}s` : `LIVE ${Math.round(global.artifact_age_seconds || 0)}s`);
  text("last-processed-bar", global.last_processed_bar_timestamp || "-");
  text("last-update", global.last_update_timestamp || "-");
  text("session-date", global.current_session_date || "-");
  text("last-refreshed", dashboard.refresh?.last_refreshed_at || "-");
  renderRuntimeBuildInfo(dashboard);
  renderOperatorSurface(dashboard.operator_surface || {});
  renderOperatorCanarySummary(paper.non_approved_lanes || {});
  renderMarketIndexStrip(market_context || {});
  renderTreasuryCurve(treasury_curve || {});
  renderMarketIndexDiagnostics(market_context || {}, dashboard.dashboard_meta || {});

  setBadge("shadow-running", shadow.running ? "RUNNING" : "STOPPED", shadow.running ? "ok" : "muted");
  setBadge("paper-running", paper.running ? "RUNNING" : "STOPPED", paper.running ? "ok" : "muted");
  text("shadow-meta", runtimeMeta(shadow));
  text("paper-meta", runtimeMeta(paper));

  const position = paper.position || {};
  text("paper-instrument", position.instrument || "-");
  setSideValue("paper-side", position.side || "FLAT");
  text("paper-qty", `${position.quantity ?? 0}`);
  text("paper-avg", position.average_price || "-");
  text("paper-mark", position.latest_bar_close || "N/A");
  setPnlValue("paper-realized", position.realized_pnl, "Generate paper summary");
  setPnlValue("paper-unrealized", position.unrealized_pnl, "N/A");
  setPnlValue("paper-session-pnl", position.session_pnl, "Generate paper summary");
  text("paper-pnl-branches", renderBranchPnl(position.pnl_by_branch));
  text("paper-pnl-note", (position.notes || []).join(" "));
  text("paper-realized-source", position.provenance?.realized || "-");
  text("paper-unrealized-source", position.provenance?.unrealized || "-");
  text("paper-branch-source", position.provenance?.branch || "-");
  text("hero-latest-state", paper.session_shape?.current_session ? "CURRENT SESSION" : (paper.session_shape?.session_date ? `LATEST ${paper.session_shape.session_date}` : "LATEST SESSION"));
  text("hero-shape-glance", paper.session_shape?.shape_label || "-");
  text(
    "hero-driver-glance",
    paper.branch_session_contribution?.top_contributor?.branch
      ? `${paper.branch_session_contribution.top_contributor.branch} ${paper.branch_session_contribution.top_contributor.total_contribution || ""}`.trim()
      : "No clear driver"
  );
  text("hero-trend-glance", paper.history?.comparison?.trend || "-");
  text("hero-exposure-glance", paper.performance?.open_exposure_summary || "-");
  renderPaperPerformance(paper.performance || {});
  renderPaperHistory(paper.history || {});
  renderPaperSessionShape(paper.session_shape || {});
  renderBranchSessionContribution(paper.branch_session_contribution || {});
  renderPaperSessionEventTimeline(paper.session_event_timeline || {});
  renderPaperModeStatus(paper.readiness || {});
  renderPaperFillAlerts();
  renderPaperEntryEligibility(paper.entry_eligibility || {});
  renderPaperActivityProof(paper.activity_proof || {});
  renderApprovedModels(paper.approved_models || {}, paper.entry_eligibility || {}, paper.temporary_paper_strategies || {});
  renderTemporaryPaperStrategies(paper.temporary_paper_strategies || {});
  renderTrackedPaperStrategies(paper.tracked_strategies || {});
  renderPaperNonApprovedLanes(paper.non_approved_lanes || {});
  renderPaperLaneActivity(paper.lane_activity || {}, paper.temporary_paper_strategies || {});
  renderPaperExceptions(paper.exceptions || {});
  renderPaperSoakSession(paper.soak_session || {});
  renderPaperRiskBanner(paper_risk_state || {});
  renderPaperCloseout(paper_closeout || {});
  renderPaperSessionCloseReview(paper_session_close_review || {});
  renderCarryForward(paper_carry_forward || {});
  renderPreSessionReview(global, paper_pre_session_review || {}, paper_carry_forward || {});
  renderPaperRunStart(paper_run_start || {});
  renderPaperContinuity(paper_continuity || {});
  renderHistoricalPlayback(historical_playback || {});
  renderLaneRegistrySections(dashboard.lane_registry || {});

  text("trail-mode", active_decision_mode.toUpperCase());
  const trailSource = active_decision_mode === "paper" ? paper.events : shadow.events;
  renderTrail("branch-trail", trailSource.branch_sources, ["bar_end_ts", "source", "decision", "block_reason"]);
  renderTrail("rule-trail", trailSource.rule_blocks, ["bar_end_ts", "source", "block_reason"]);
  renderTrail("alert-trail", trailSource.alerts, ["logged_at", "severity", "code", "message"]);
  renderTrail("reconcile-trail", trailSource.reconciliation, ["logged_at", "clean", "issues"]);
  renderTrail("operator-control-trail", paper.events.operator_controls, ["requested_at", "action", "status", "message"]);

  renderTable("blotter-table", paper.latest_blotter_rows, [
    "entry_ts", "exit_ts", "direction", "setup_family", "entry_px", "exit_px", "net_pnl", "exit_reason",
  ], state.filters.blotter);
  renderTable("fills-table", paper.latest_fills, [
    "fill_timestamp", "order_intent_id", "intent_type", "fill_price", "order_status", "broker_order_id",
  ], state.filters.fills);
  renderTable("intents-table", paper.latest_intents, [
    "created_at", "order_intent_id", "intent_type", "reason_code", "order_status", "broker_order_id",
  ], state.filters.intents);
  renderTable("historical-playback-table", historical_playback?.latest_run?.rows, [
    "symbol", "lane_family", "side", "bars_processed", "signals_seen", "intents_created", "fills_created", "first_trigger_timestamp", "first_fill_timestamp", "result_status", "block_or_fault_reason",
  ], state.filters.historicalPlayback);
  renderTable("branch-performance-table", paper.branch_session_contribution?.rows, [
    "branch", "realized_pnl", "unrealized_pnl", "total_contribution", "fills", "closed_trades", "first_meaningful_time", "last_meaningful_time", "net_effect", "timing_hint", "path_hint", "scope",
  ], state.filters.branchSession);
  renderTable("branch-history-table", paper.history?.branch_history, [
    "branch", "sessions_seen", "realized_pnl", "signals", "blocked", "closed_trades", "win_rate", "stability", "scope",
  ], state.filters.branchHistory);
  renderTable("recent-trades-table", paper.performance?.recent_trades, [
    "timestamp", "instrument", "side", "entry_px", "exit_px", "realized_pnl", "source", "status",
  ], state.filters.recent);
  renderTable("session-history-table", paper.history?.recent_sessions, [
    "session_date", "realized_pnl", "total_pnl", "trade_count", "fill_count", "win_count", "loss_count", "close_state", "major_contributors_label",
  ], state.filters.sessionHistory);

  renderReviewCard("shadow", review.shadow);
  renderReviewCard("paper", review.paper);
  renderActionLog(action_log || []);
  renderManualControls(manual_controls.controls || []);
  setRefreshSelect();
  setButtonsBusy();
}

function runtimeMeta(runtime) {
  const status = runtime.status || {};
  const process = runtime.process || {};
  const control = runtime.latest_operator_control || {};
  return [
    `Process: ${runtime.running ? "RUNNING" : "STOPPED"}${process.pid ? ` (PID ${process.pid})` : ""}`,
    `Backgrounded: ${process.backgrounded ? "yes" : "no"}`,
    `Freshness: ${status.freshness || "-"}${status.artifact_age_seconds != null ? ` (${Math.round(status.artifact_age_seconds)}s)` : ""}`,
    `Health: ${status.health_status}`,
    `Strategy: ${status.strategy_status}`,
    `Market data: ${status.market_data_semantics}`,
    `Reconciliation: ${status.reconciliation_semantics}`,
    `Fault: ${status.fault_state}${status.fault_code ? ` (${status.fault_code})` : ""}`,
    `Entries enabled: ${status.entries_enabled ? "yes" : "no"}`,
    `Operator halt: ${status.operator_halt ? "yes" : "no"}`,
    `Position: ${status.position_side}`,
    `Last bar: ${status.last_processed_bar_end_ts || "-"}`,
    `Last update: ${status.last_update_ts || "-"}`,
    `PID file: ${process.pid_file || "-"}`,
    `Log file: ${process.log_file || "-"}`,
    `Artifacts: ${process.artifacts_dir || "-"}`,
    `Can stop: ${process.can_stop ? "yes" : "no"}`,
    control.action ? `Latest control: ${control.action} -> ${control.status || "-"} at ${control.applied_at || control.requested_at || "-"}` : "Latest control: -",
  ].join("\n");
}

function renderBranchPnl(pnlByBranch) {
  const entries = Object.entries(pnlByBranch || {});
  if (!entries.length) return "Generate paper summary";
  return entries.map(([branch, pnl]) => `${branch}: ${pnl}`).join(" | ");
}

function renderTrail(id, rows, fields) {
  const target = document.getElementById(id);
  target.innerHTML = "";
  if (!rows || !rows.length) {
    target.innerHTML = "<li>No recent events.</li>";
    return;
  }
  rows
    .slice()
    .reverse()
    .forEach((row) => {
      const item = document.createElement("li");
      item.textContent = fields
        .map((field) => `${field}=${Array.isArray(row[field]) ? row[field].join("|") : row[field] ?? "-"}`)
        .join(" • ");
      target.appendChild(item);
    });
}

function renderMarketIndexStrip(payload) {
  setBadge("market-strip-feed-state", payload.feed_label || "INDEX FEED UNKNOWN", marketFeedLevel(payload.feed_state));
  text("market-strip-note", payload.note || "-");
  setLink("market-strip-diagnostics-link", payload.diagnostic_artifact || null);
  const rowByLabel = new Map((payload.symbols || []).map((row) => [String(row.label || "").toUpperCase(), row]));
  ["DJIA", "SPX", "NDX", "RUT", "GOLD", "VIX"].forEach((label) => {
    const row = rowByLabel.get(label) || {
      state: "UNAVAILABLE",
      value_state: "UNAVAILABLE",
      current_value: null,
      absolute_change: null,
      percent_change: null,
      bid: null,
      ask: null,
      bid_state: "UNAVAILABLE",
      ask_state: "UNAVAILABLE",
      note: "No market-index payload.",
      display_symbol: label,
    };
    const key = label.toLowerCase();
    const stateLabel = row.value_state === "UNAVAILABLE" ? (row.state || "UNAVAILABLE") : (row.value_state || row.state || "UNAVAILABLE");
    setBadge(`market-state-${key}`, stateLabel, marketFeedLevel(stateLabel));
    text(`market-value-${key}`, row.current_value || "Unavailable");
    setSignedValue(`market-change-${key}`, [row.absolute_change || "Unavailable", row.percent_change || "Unavailable"].join(" / "));
    text(`market-bid-${key}`, row.bid || "Unavailable");
    text(`market-ask-${key}`, row.ask || "Unavailable");
    const noteParts = [];
    if (row.display_symbol) {
      noteParts.push(row.display_symbol);
    }
    if (row.bid_state === "UNAVAILABLE" || row.ask_state === "UNAVAILABLE") {
      noteParts.push("Bid/ask unavailable");
    }
    if (row.note) {
      noteParts.push(row.note);
    }
    text(`market-note-${key}`, noteParts.join(" • "));
  });
}

function renderMarketIndexDiagnostics(payload, dashboardMeta) {
  const debug = payload.debug || {};
  text("market-debug-pid", debug.server_pid != null ? String(debug.server_pid) : "-");
  text("market-debug-started", debug.server_started_at || "-");
  text("market-debug-url", debug.server_url || "-");
  text(
    "market-debug-hostport",
    debug.server_host && debug.server_port != null ? `${debug.server_host}:${debug.server_port}` : "-"
  );
  text("market-debug-snapshot-path", debug.snapshot_file_path || "-");
  text("market-debug-diagnostics-path", debug.diagnostics_file_path || "-");
  text("market-debug-updated", debug.snapshot_updated_at || payload.updated_at || "-");
  setBadge("market-debug-build", `BUILD ${debug.build_stamp || dashboardMeta.build_stamp || "-"}`, "info");
  setLink("market-strip-snapshot-link", debug.snapshot_artifact || payload.snapshot_artifact || null);
  setLink("market-strip-diagnostics-link-raw", debug.diagnostic_artifact || payload.diagnostic_artifact || null);

  const table = document.getElementById("market-debug-table");
  if (!table) return;
  const rows = Array.isArray(debug.symbols) ? debug.symbols : [];
  if (!rows.length) {
    table.innerHTML = "<tr><td colspan=\"9\">No market-strip diagnostics yet.</td></tr>";
    return;
  }
  table.innerHTML = rows
    .map((row) => {
      return `<tr>
        <td>${escapeHtml(row.label ?? "-")}</td>
        <td>${escapeHtml(row.requested_symbol ?? "-")}</td>
        <td>${escapeHtml(row.matched_symbol ?? "-")}</td>
        <td>${escapeHtml(row.render_classification ?? "-")}</td>
        <td>${escapeHtml(yesNo(row.current_present))}</td>
        <td>${escapeHtml(yesNo(row.change_present))}</td>
        <td>${escapeHtml(yesNo(row.percent_change_present))}</td>
        <td>${escapeHtml(yesNo(row.bid_present))}</td>
        <td>${escapeHtml(yesNo(row.ask_present))}</td>
      </tr>`;
    })
    .join("");
}

function renderRuntimeBuildInfo(dashboard) {
  const meta = dashboard.dashboard_meta || {};
  const operatorSurface = dashboard.operator_surface || {};
  const activeSurface = operatorSurface.active_instrument_surface || {};
  const classificationCounts = activeSurface.classification_counts || {};
  const approvedQuantCount = classificationCounts.approved_quant || 0;
  const admittedPaperCount = classificationCounts.admitted_paper || 0;
  const temporaryPaperCount = classificationCounts.temporary_paper || 0;
  text("runtime-build-stamp", meta.build_stamp || "-");
  text("runtime-server-pid", meta.server_pid != null ? String(meta.server_pid) : "-");
  text("runtime-started-at", meta.server_started_at || "-");
  text("runtime-snapshot-generated", dashboard.generated_at || dashboard.refresh?.last_refreshed_at || "-");
  text("runtime-approved-quant-count", String(approvedQuantCount));
  text("runtime-admitted-paper-count", String(admittedPaperCount));
  text("runtime-temporary-paper-count", String(temporaryPaperCount));
  const line = `RUNTIME ${meta.build_stamp || "-"} | pid=${meta.server_pid ?? "-"} | approved_quant=${approvedQuantCount} | admitted_paper=${admittedPaperCount} | temporary_paper=${temporaryPaperCount} | active_instruments=${activeSurface.active_instruments_count || 0} | active_lanes=${activeSurface.active_lanes_count || 0}`;
  text("runtime-registry-line", line);
}

function renderOperatorCanarySummary(payload) {
  const rows = (Array.isArray(payload.rows) ? payload.rows : []).filter((row) => row && (row.experimental_status === "experimental_canary" || row.is_canary));
  const killSwitchActive = Boolean(payload.kill_switch_active);
  const enabledCount = rows.filter((row) => String(row.state || "").toUpperCase() === "ENABLED").length;
  setBadge(
    "operator-canary-badge",
    rows.length ? (killSwitchActive ? "KILL SWITCH ACTIVE" : (enabledCount ? "VISIBLE NOW" : "DISABLED")) : "NO TEMP PAPER",
    rows.length ? (killSwitchActive ? "danger" : (enabledCount ? "warning" : "muted")) : "muted",
  );
  setLink("operator-canary-link", "/api/operator-artifact/paper-temporary-paper-strategies");
  text(
    "operator-canary-status",
    rows.length
      ? `${payload.operator_state_label || "PAPER ONLY"} • ${killSwitchActive ? "Kill switch active" : "Kill switch inactive"}`
      : "No experimental canary lanes are surfaced in this runtime."
  );
  text("operator-canary-visible-count", String(rows.length));
  text("operator-canary-enabled-count", String(enabledCount));
  text("operator-canary-kill-switch", killSwitchActive ? "ACTIVE" : "INACTIVE");
  text("operator-canary-signal-count", String(rows.reduce((sum, row) => sum + Number(row.recent_signal_count || row.signal_count || 0), 0)));
  text("operator-canary-event-count", String(rows.reduce((sum, row) => sum + Number(row.recent_event_count || row.event_count || 0), 0)));
  text(
    "operator-canary-note",
    rows.length
      ? (payload.note || "Experimental temporary paper strategies are visible here for operator monitoring.")
      : "No temporary paper strategies are enabled in this runtime."
  );

  const container = document.getElementById("operator-canary-cards");
  if (!container) return;
  if (!rows.length) {
    container.innerHTML = `<article class="operator-canary-card"><div class="operator-canary-summary-list"><span>No temporary paper strategies are currently enabled.</span><span>Shared paper runtime and restart actions are aligned with zero requested temp-paper lanes.</span></div></article>`;
    return;
  }

  container.innerHTML = rows
    .slice()
    .sort((left, right) => String(left.display_name || left.branch || "").localeCompare(String(right.display_name || right.branch || "")))
    .map((row) => {
      const title = row.display_name || row.branch || row.lane_id || "-";
      const state = String(row.state || "UNKNOWN").toUpperCase();
      const latestAtpState = row.latest_atp_state || {};
      const latestAtpEntryState = row.latest_atp_entry_state || {};
      const latestAtpTimingState = row.latest_atp_timing_state || {};
      const stateLevel = killSwitchActive ? "danger" : (state === "ENABLED" ? "ok" : "muted");
      const statusSummary = [
        row.experimental_status === "experimental_canary" ? "Experimental" : null,
        row.paper_only ? "Paper Only" : null,
        row.non_approved ? "Non-Approved" : null,
        row.quality_bucket_policy ? `Quality ${row.quality_bucket_policy}` : null,
        row.side ? `Side ${row.side}` : null,
      ].filter(Boolean).join(" • ");
      return `
        <article class="operator-canary-card">
          <div class="operator-canary-card-header">
            <div>
              <div class="operator-canary-card-title">${escapeHtml(title)}</div>
              <div class="operator-canary-card-subtitle mono">${escapeHtml(row.lane_id || "-")}</div>
            </div>
            <span class="${badgeClass(stateLevel)}">${escapeHtml(state)}</span>
          </div>
          <div class="operator-canary-chip-row">
            <span class="operator-canary-chip"><strong>Status</strong> ${escapeHtml(statusSummary || "Experimental • Paper Only • Non-Approved")}</span>
            <span class="operator-canary-chip"><strong>Kill Switch</strong> ${escapeHtml(killSwitchActive ? "ACTIVE" : "INACTIVE")}</span>
          </div>
          <div class="operator-canary-chip-row">
            <span class="operator-canary-chip"><strong>Signals</strong> ${escapeHtml(String(row.recent_signal_count || row.signal_count || 0))}</span>
            <span class="operator-canary-chip"><strong>Events</strong> ${escapeHtml(String(row.recent_event_count || row.event_count || 0))}</span>
          </div>
          <div class="operator-canary-chip-row">
            <span class="operator-canary-chip"><strong>ATP Bias</strong> ${escapeHtml(String(latestAtpState.bias_state || "-"))}</span>
            <span class="operator-canary-chip"><strong>ATP Pullback</strong> ${escapeHtml(String(latestAtpState.pullback_state || "-"))}</span>
          </div>
          <div class="operator-canary-chip-row">
            <span class="operator-canary-chip"><strong>ATP Entry</strong> ${escapeHtml(String(latestAtpEntryState.entry_state || "-"))}</span>
            <span class="operator-canary-chip"><strong>ATP Blocker</strong> ${escapeHtml(String(latestAtpEntryState.primary_blocker || "-"))}</span>
          </div>
          <div class="operator-canary-chip-row">
            <span class="operator-canary-chip"><strong>ATP Timing</strong> ${escapeHtml(String(latestAtpTimingState.timing_state || "-"))}</span>
            <span class="operator-canary-chip"><strong>ATP VWAP</strong> ${escapeHtml(String(latestAtpTimingState.vwap_price_quality_state || "-"))}</span>
          </div>
          <div class="operator-canary-summary-list">
            <span>${escapeHtml(row.operator_status_line || statusSummary || "-")}</span>
            <span>${escapeHtml(`Depth ${latestAtpState.pullback_depth_score ?? "-"} | Violence ${latestAtpState.pullback_violence_score ?? "-"}` + (latestAtpState.pullback_reason ? ` | ${latestAtpState.pullback_reason}` : "") + ` | Trigger ${latestAtpEntryState.continuation_trigger_state || "-"}` + ` | Timing ${latestAtpTimingState.primary_blocker || latestAtpTimingState.timing_state || "-"}`)}</span>
            <span>${escapeHtml(row.note || "Experimental temporary paper strategy for dashboard observation.")}</span>
          </div>
        </article>
      `;
    })
    .join("");
}

function renderOperatorSurface(payload) {
  renderOperatorReadiness(payload.runtime_readiness || payload.readiness || {});
  renderOperatorRiskStrip(payload.operator_metrics_portfolio || payload.daily_risk || {});
  renderOperatorInstrumentRollup(payload.operator_metrics_by_instrument || {});
  renderOperatorActivePositions(payload.current_active_positions || {});
  renderOperatorUniverse(payload.active_instrument_surface || payload.lane_universe || {}, payload.lane_rows || []);
  renderOperatorLaneGrid((payload.active_instrument_surface || {}).rows || payload.lane_rows || []);
  renderOperatorContext(payload.secondary_context || payload.context || {});
}

function renderOperatorReadiness(payload) {
  text("operator-readiness-status", payload.status_line || "-");
  const cards = document.getElementById("operator-readiness-cards");
  if (cards) {
    cards.innerHTML = renderOperatorMetricCards(buildOperatorReadinessCards(payload));
  }
  const notes = document.getElementById("operator-readiness-notes");
  if (notes) {
    const rows = buildOperatorReadinessNotes(payload);
    notes.innerHTML = rows.length
      ? rows.map((row) => `<li>${escapeHtml(row)}</li>`).join("")
      : "<li>No readiness notes available.</li>";
  }
}

function renderOperatorRiskStrip(payload) {
  text("operator-risk-status", payload.status_line || "-");
  setLink("operator-risk-link", payload.artifact_href || null);
  const cards = document.getElementById("operator-risk-cards");
  if (cards) {
    cards.innerHTML = renderOperatorMetricCards(buildOperatorPortfolioCards(payload));
  }
  const notes = document.getElementById("operator-risk-notes");
  if (notes) {
    const rows = buildOperatorPortfolioNotes(payload);
    notes.innerHTML = rows.length
      ? rows.map((row) => `<li>${escapeHtml(row)}</li>`).join("")
      : "<li>No portfolio horizon notes available.</li>";
  }
}

function renderOperatorUniverse(payload, rows) {
  text("operator-universe-status", payload.status_line || "-");
  const instrumentSummary = Array.from(new Set((payload.rows || []).map((row) => row.instrument).filter(Boolean)));
  text("operator-universe-instruments", instrumentSummary.join(", ") || "-");
  text("operator-universe-note", payload.table_note || payload.status_line || "-");
  setLink("operator-universe-link", "/api/operator-artifact/operator-surface");
  const cards = document.getElementById("operator-universe-cards");
  if (cards) {
    cards.innerHTML = renderOperatorMetricCards(buildOperatorUniverseCards(payload));
  }
  const summary = document.getElementById("operator-lane-grid-summary");
  if (summary) {
    summary.textContent = payload.status_line || `${lenOrZero(rows)} unified lane rows`;
  }
}

function renderOperatorLaneGrid(rows) {
  const table = document.getElementById("operator-lane-grid-table");
  if (!table) return;
  const safeRows = Array.isArray(rows) ? rows : [];
  if (!safeRows.length) {
    table.innerHTML = "<tr><td>No surfaced lanes available.</td></tr>";
    return;
  }
  table.className = "approved-models-table-compact operator-lane-grid-table";
  table.innerHTML = `
    <thead>
      <tr>
        <th>instrument</th>
        <th>lane</th>
        <th>class</th>
        <th>state</th>
        <th>side</th>
        <th>session</th>
        <th>net</th>
        <th>dd</th>
        <th>pos</th>
        <th>sig</th>
        <th>latest</th>
        <th>warnings</th>
      </tr>
    </thead>
    <tbody>
      ${safeRows
        .map((row) => `
          <tr>
            <td class="approved-lane-instrument mono">${escapeHtml(row.instrument || "-")}</td>
            <td class="approved-lane-family">
              <div class="approved-lane-family-wrap">
                <span class="approved-lane-family-name mono">${escapeHtml(row.family || row.classification || "-")}</span>
                <span class="approved-lane-label subnote mono">${escapeHtml(row.display_name || row.lane_id || "-")}</span>
                <span class="approved-lane-exit subnote mono">${escapeHtml(row.active_exit || "-")}</span>
              </div>
            </td>
            <td><span class="${badgeClass(classificationLevel(row.classification_tag || row.classification || "unknown"))}">${escapeHtml(row.classification || "-")}</span></td>
            <td><span class="${badgeClass(row.blocked ? "danger" : row.open_position ? "warning" : "info")}">${escapeHtml(row.state || row.current_state || "-")}</span></td>
            <td>${escapeHtml(row.side || "-")}</td>
            <td>${sessionTagMarkup(row.session || row.session_label || "-")}</td>
            <td>${escapeHtml(row.current_net_pnl ?? row.today_contribution ?? "Unavailable")}</td>
            <td>${escapeHtml(row.current_session_max_drawdown ?? "Unavailable")}</td>
            <td>${escapeHtml(row.open_position ? "1" : "0")}</td>
            <td>${escapeHtml(row.signaled_today ? "1" : "0")}</td>
            <td class="mono">${escapeHtml(row.latest_timestamp || "-")}</td>
            <td>${escapeHtml(row.warning_summary || "No active warnings.")}</td>
          </tr>
        `)
        .join("")}
    </tbody>
  `;
}

function renderOperatorContext(payload) {
  text("operator-context-status", payload.status_line || "-");
  const container = document.getElementById("operator-context-items");
  if (!container) return;
  const items = Array.isArray(payload.items) ? payload.items : [];
  if (!items.length) {
    container.innerHTML = `<article class="lane-surface-card"><div class="subnote">No context items available.</div></article>`;
    return;
  }
  const primaryMarkup = items
    .map((item) => `
      <article class="operator-context-item" title="${escapeHtml(`Source: ${item.source || "-"} | Last refresh: ${item.last_refresh_timestamp || "-"}`)}">
        <div class="lane-surface-card-top">
          <div class="lane-surface-card-title">${escapeHtml(item.label || "-")}</div>
          <span class="${badgeClass(item.status_level || contextStatusLevel(item.status || (item.available ? "live" : "unavailable")))}">${escapeHtml((item.status_label || item.status || "unavailable").toUpperCase())}</span>
        </div>
        <div class="operator-context-value mono">${escapeHtml(item.value_label || item.value || "-")}</div>
        <div class="operator-context-reference ${item.reference_value ? "" : "hidden"}">Reference: ${escapeHtml(item.reference_value || "-")}</div>
        <div class="operator-context-meta">Freshness: ${escapeHtml(item.last_refresh_timestamp || "-")}</div>
        <div class="operator-context-note ${item.reason ? "" : "hidden"}">${escapeHtml(item.reason || "")}</div>
      </article>
    `)
    .join("");
  container.innerHTML = primaryMarkup;
}

function renderOperatorInstrumentRollup(payload) {
  text("operator-instrument-status", payload.status_line || "-");
  const table = document.getElementById("operator-instrument-table");
  if (!table) return;
  const rows = Array.isArray(payload.rows) ? payload.rows : [];
  if (!rows.length) {
    table.innerHTML = "<tr><td>No instrument rollup available.</td></tr>";
    return;
  }
  const optionalHorizonColumns = [
    { key: "lifetime", label: "lifetime" },
    { key: "ytd", label: "YTD" },
    { key: "mtd", label: "MTD" },
    { key: "yesterday", label: "yesterday" },
  ].filter((column) => rows.some((row) => horizonAvailable((row.realized_pnl_horizons || {})[column.key])));
  table.className = "approved-models-table-compact operator-instrument-table";
  table.innerHTML = `
    <thead>
      <tr>
        <th>instrument</th>
        <th>classes</th>
        ${optionalHorizonColumns.map((column) => `<th>${escapeHtml(column.label)}</th>`).join("")}
        <th>today</th>
        <th>unrealized</th>
        <th>net</th>
        <th>session_dd</th>
        <th>positions</th>
        <th>signals</th>
        <th>blocked</th>
        <th>latest</th>
        <th>warnings</th>
      </tr>
    </thead>
    <tbody>
      ${rows.map((row) => `
        <tr>
          <td class="mono">${escapeHtml(row.instrument || "-")}</td>
          <td>${escapeHtml((row.classification_mix || []).join(", ") || "-")}</td>
          ${optionalHorizonColumns
            .map((column) => `<td>${escapeHtml(operatorHorizonValue((row.realized_pnl_horizons || {})[column.key]))}</td>`)
            .join("")}
          <td>${escapeHtml(operatorHorizonValue(row.realized_pnl_horizons?.today, row.realized_pnl))}</td>
          <td>${escapeHtml(row.unrealized_pnl ?? "Unavailable")}</td>
          <td>${escapeHtml(row.net_pnl ?? "Unavailable")}</td>
          <td>${escapeHtml(row.current_session_max_drawdown ?? "Unavailable")}</td>
          <td>${escapeHtml(String(row.active_position_count ?? 0))}</td>
          <td>${escapeHtml(String(row.active_signal_count ?? 0))}</td>
          <td>${escapeHtml(String(row.blocked_lane_count ?? 0))}</td>
          <td class="mono">${escapeHtml(row.latest_activity_timestamp || "-")}</td>
          <td>${escapeHtml(row.warning_summary || "No active warnings.")}</td>
        </tr>`).join("")}
    </tbody>
  `;
}

function renderOperatorActivePositions(payload) {
  text("operator-active-positions-status", payload.status_line || "-");
  const table = document.getElementById("operator-active-positions-table");
  if (!table) return;
  const rows = Array.isArray(payload.rows) ? payload.rows : [];
  if (!rows.length) {
    table.className = "approved-models-table-compact operator-active-positions-table operator-empty-table";
    table.innerHTML = "<tbody><tr><td colspan=\"9\">No active positions.</td></tr></tbody>";
    return;
  }
  table.className = "approved-models-table-compact operator-active-positions-table";
  table.innerHTML = `
    <thead>
      <tr>
        <th>instrument</th>
        <th>class</th>
        <th>lane</th>
        <th>side</th>
        <th>qty</th>
        <th>net</th>
        <th>entry</th>
        <th>exit</th>
        <th>risk</th>
        <th>warnings</th>
      </tr>
    </thead>
    <tbody>
      ${rows.map((row) => `
        <tr>
          <td class="mono">${escapeHtml(row.instrument || "-")}</td>
          <td>${escapeHtml(row.classification || "-")}</td>
          <td>
            <div class="approved-lane-family-wrap">
              <span class="approved-lane-family-name mono">${escapeHtml(row.display_name || row.lane_id || "-")}</span>
              <span class="approved-lane-label subnote mono">${escapeHtml(row.entry_timestamp || "Unavailable")}</span>
            </div>
          </td>
          <td>${escapeHtml(row.side || "-")}</td>
          <td>${escapeHtml(String(row.quantity ?? "-"))}</td>
          <td>${escapeHtml(row.net_pnl ?? "Unavailable")}</td>
          <td>${escapeHtml(row.entry_basis ?? "Unavailable")}</td>
          <td>${escapeHtml(row.active_exit || "-")}</td>
          <td>${escapeHtml(row.open_risk_state || "-")}</td>
          <td>${escapeHtml(row.warning_summary || "No active warnings.")}</td>
        </tr>`).join("")}
    </tbody>
  `;
}

function renderOperatorMetricCards(cards) {
  const safeCards = Array.isArray(cards) ? cards : [];
  if (!safeCards.length) {
    return `<div class="stat wide"><span class="label">Status</span><span class="value mono">Unavailable</span></div>`;
  }
  return safeCards
    .map(
      (card) => `
        <div class="stat">
          <span class="label">${escapeHtml(card.label || "-")}</span>
          <span class="value mono">${escapeHtml(card.value ?? "-")}</span>
        </div>
      `,
    )
    .join("");
}

function buildOperatorPortfolioCards(payload) {
  const values = payload.values || payload;
  const horizons = values.realized_pnl_horizons || {};
  const optionalHorizonCards = [
    { key: "lifetime", label: "Lifetime Realized" },
    { key: "ytd", label: "YTD Realized" },
    { key: "mtd", label: "MTD Realized" },
    { key: "yesterday", label: "Yesterday Realized" },
  ]
    .filter((row) => horizonAvailable(horizons[row.key]))
    .map((row) => ({ label: row.label, value: operatorHorizonValue(horizons[row.key]) }));
  return [
    { label: "Today Realized", value: operatorHorizonValue(horizons.today, values.daily_realized_pnl) },
    { label: "Current Unrealized", value: values.daily_unrealized_pnl ?? "Unavailable" },
    { label: "Current Net", value: values.daily_net_pnl ?? "Unavailable" },
    { label: "Session Max DD", value: values.intraday_max_drawdown ?? "Unavailable" },
    { label: "Active Positions", value: String(values.active_positions_count ?? 0) },
    { label: "Active Signals", value: String(values.active_signals_count ?? 0) },
    { label: "Blocked Lanes", value: String(values.blocked_lanes_count ?? 0) },
    { label: "Active Instruments", value: String(values.active_instruments_count ?? 0) },
    ...optionalHorizonCards,
  ];
}

function horizonAvailable(horizon) {
  return Boolean(horizon && horizon.available && horizon.value != null && horizon.value !== "");
}

function buildOperatorReadinessCards(payload) {
  const values = payload.values || payload;
  return [
    { label: "System Health", value: values.runtime_status || "Unavailable" },
    { label: "Paper Runtime", value: values.paper_enabled ? "RUNNING" : "STOPPED" },
    { label: "Entries", value: values.entries_enabled ? "ENABLED" : "HALTED" },
    { label: "Auth", value: values.auth_readiness ? "READY" : "NOT_READY" },
    { label: "Market Data", value: values.market_data_readiness || "Unavailable" },
    { label: "Faults", value: String(values.blocking_faults_count ?? 0) },
    { label: "Active Lanes", value: String(values.active_lanes_count ?? 0) },
    { label: "Active Instruments", value: String(values.active_instruments_count ?? 0) },
  ];
}

function buildOperatorReadinessNotes(payload) {
  const values = payload.values || payload;
  const rows = [];
  rows.push(`Auth readiness: ${values.auth_readiness ? "READY" : "NOT_READY"}`);
  rows.push(`Degraded informational feeds: ${(values.degraded_informational_feeds || []).join(", ") || "None"}`);
  const faults = Array.isArray(payload.blocking_faults) ? payload.blocking_faults : [];
  faults.forEach((row) => {
    rows.push(`Fault ${row.code || "-"}: ${row.summary || "-"}${row.owner ? ` (${row.owner})` : ""}`);
  });
  return rows;
}

function buildOperatorPortfolioNotes(payload) {
  const values = payload.values || payload;
  const horizons = values.realized_pnl_horizons || {};
  const notes = [];
  ["lifetime", "ytd", "mtd", "yesterday", "today"].forEach((key) => {
    const row = horizons[key];
    if (!row || row.available) return;
    notes.push(`${key.toUpperCase()}: ${row.source_gap || "Unavailable from current operator artifacts."}`);
  });
  return notes;
}

function buildOperatorUniverseCards(payload) {
  return [
    { label: "Total Instruments", value: String(payload.active_instruments_count ?? 0) },
    { label: "Total Lanes", value: String(payload.active_lanes_count ?? 0) },
    { label: "Approved Quant", value: String((payload.classification_counts || {}).approved_quant ?? 0) },
    { label: "Admitted Paper", value: String((payload.classification_counts || {}).admitted_paper ?? 0) },
    { label: "Temp Paper", value: String((payload.classification_counts || {}).temporary_paper ?? 0) },
    { label: "Canary", value: String((payload.classification_counts || {}).canary ?? 0) },
  ];
}

function operatorHorizonValue(horizon, fallback = null) {
  if (horizon && horizon.available && horizon.value != null) return String(horizon.value);
  if (fallback != null && fallback !== "") return String(fallback);
  return "Unavailable";
}

function renderTreasuryCurve(payload) {
  setBadge("treasury-feed-state", payload.feed_label || "TREASURY CURVE UNKNOWN", marketFeedLevel(payload.feed_state || payload.panel_classification));
  text("treasury-updated-at", payload.updated_at || "-");
  setLink("treasury-curve-snapshot-link", payload.snapshot_artifact || null);
  setLink("treasury-curve-diagnostics-link", payload.diagnostic_artifact || null);
  setLink("treasury-curve-audit-link", payload.audit_artifact || null);

  const summary = payload.summary || {};
  const tenorSummary = summary.tenor_summary || {};
  text("treasury-summary-3m", tenorSummary["3M"] ? `${tenorSummary["3M"]}%` : "Unavailable");
  text("treasury-summary-5y", tenorSummary["5Y"] ? `${tenorSummary["5Y"]}%` : "Unavailable");
  text("treasury-summary-10y", tenorSummary["10Y"] ? `${tenorSummary["10Y"]}%` : "Unavailable");
  text("treasury-summary-30y", tenorSummary["30Y"] ? `${tenorSummary["30Y"]}%` : "Unavailable");
  text("treasury-summary-3m10y", formatSpreadValue(summary.spreads?.["3M10Y"]));
  text("treasury-summary-5s30s", formatSpreadValue(summary.spreads?.["5s30s"]));
  text("treasury-summary-10s30s", formatSpreadValue(summary.spreads?.["10s30s"]));
  setStatusValue("treasury-curve-state", summary.curve_state_label || "INSUFFICIENT DATA", levelForCurveState(summary.curve_state_label));

  text("treasury-panel-note", payload.curve_note || "-");
  const rows = Array.isArray(payload.tenors) ? payload.tenors : [];
  const liveCount = rows.filter((row) => row.current_state === "LIVE" || row.current_state === "DELAYED").length;
  const priorCount = rows.filter((row) => row.prior_state === "AVAILABLE").length;
  text("treasury-availability-summary", `${liveCount}/${rows.length} live • ${priorCount}/${rows.length} prior`);
  text(
    "treasury-missing-note",
    payload.coverage_note || "Full direct 1M-30Y Schwab coverage was not verified on this path."
  );
  text(
    "treasury-chart-note",
    `${payload.feed_source || "-"} ${payload.chart?.gap_policy || ""}`.trim(),
  );

  const table = document.getElementById("treasury-tenor-table");
  if (!table) return;
  if (!rows.length) {
    table.innerHTML = "<tr><td colspan=\"6\">No Treasury curve data yet.</td></tr>";
  } else {
    table.innerHTML = rows
      .map((row) => `
        <tr>
          <td>${escapeHtml(row.tenor || "-")}</td>
          <td>${escapeHtml(row.display_symbol || row.external_symbol || "-")}</td>
          <td>${escapeHtml(row.current_yield ? `${row.current_yield}%` : "Unavailable")}</td>
          <td>${escapeHtml(row.prior_yield ? `${row.prior_yield}%` : "Unavailable")}</td>
          <td>${escapeHtml(row.day_change_bp ? `${row.day_change_bp} bp` : "Unavailable")}</td>
          <td>${escapeHtml(row.render_classification || "-")}</td>
        </tr>
      `)
      .join("");
  }

  renderTreasuryCurveChart(payload.chart || {});
}

function renderTreasuryCurveChart(chart) {
  const svg = document.getElementById("treasury-curve-chart");
  if (!svg) return;
  while (svg.firstChild) svg.removeChild(svg.firstChild);

  const points = Array.isArray(chart.points) ? chart.points : [];
  if (!points.length) {
    svg.innerHTML = '<text x="24" y="40" fill="#9cb0c4" font-size="12">No Treasury curve data yet.</text>';
    return;
  }

  const currentValues = points.filter((point) => point.current_available).map((point) => Number(point.current_yield));
  const priorValues = points.filter((point) => point.prior_available).map((point) => Number(point.prior_yield));
  const allValues = [...currentValues, ...priorValues].filter((value) => Number.isFinite(value));
  if (!allValues.length) {
    svg.innerHTML = '<text x="24" y="40" fill="#9cb0c4" font-size="12">No current or prior-session Treasury yields are available.</text>';
    return;
  }

  const width = 1040;
  const height = 260;
  const left = 54;
  const right = 24;
  const top = 18;
  const bottom = 42;
  const innerWidth = width - left - right;
  const innerHeight = height - top - bottom;
  const minValue = Math.min(...allValues);
  const maxValue = Math.max(...allValues);
  const paddedMin = minValue - 0.1;
  const paddedMax = maxValue + 0.1;
  const yFor = (value) => {
    if (paddedMax === paddedMin) return top + innerHeight / 2;
    return top + ((paddedMax - value) / (paddedMax - paddedMin)) * innerHeight;
  };
  const xFor = (index) => {
    if (points.length <= 1) return left + innerWidth / 2;
    return left + (index / (points.length - 1)) * innerWidth;
  };

  [0, 0.25, 0.5, 0.75, 1].forEach((fraction) => {
    const value = paddedMax - (paddedMax - paddedMin) * fraction;
    const y = yFor(value);
    svg.appendChild(svgLine(left, y, width - right, y, "rgba(255,255,255,0.08)", 1));
    svg.appendChild(svgText(10, y + 4, `${value.toFixed(3)}%`, "11", "#9cb0c4"));
  });

  points.forEach((point, index) => {
    const x = xFor(index);
    svg.appendChild(svgText(x, height - 16, point.tenor || "-", "11", "#9cb0c4", "middle"));
    if (!point.current_available && !point.prior_available) {
      svg.appendChild(svgCircle(x, top + innerHeight / 2, 3, "rgba(255,123,123,0.45)"));
    }
  });

  drawTreasurySeries(svg, points, "prior_available", "prior_yield", xFor, yFor, "#ffcf5a");
  drawTreasurySeries(svg, points, "current_available", "current_yield", xFor, yFor, "#6dcff6");
}

function drawTreasurySeries(svg, points, availabilityKey, valueKey, xFor, yFor, color) {
  let segment = [];
  points.forEach((point, index) => {
    if (point[availabilityKey] && Number.isFinite(Number(point[valueKey]))) {
      segment.push([xFor(index), yFor(Number(point[valueKey]))]);
    } else {
      flushTreasurySegment(svg, segment, color);
      segment = [];
    }
  });
  flushTreasurySegment(svg, segment, color);

  points.forEach((point, index) => {
    if (!(point[availabilityKey] && Number.isFinite(Number(point[valueKey])))) return;
    const x = xFor(index);
    const y = yFor(Number(point[valueKey]));
    svg.appendChild(svgCircle(x, y, 4, color));
  });
}

function flushTreasurySegment(svg, segment, color) {
  if (segment.length < 2) return;
  const polyline = document.createElementNS("http://www.w3.org/2000/svg", "polyline");
  polyline.setAttribute("fill", "none");
  polyline.setAttribute("stroke", color);
  polyline.setAttribute("stroke-width", "2");
  polyline.setAttribute("stroke-linecap", "round");
  polyline.setAttribute("stroke-linejoin", "round");
  polyline.setAttribute("points", segment.map((point) => point.join(",")).join(" "));
  svg.appendChild(polyline);
}

function renderTable(id, rows, fields, filterText = "") {
  const table = document.getElementById(id);
  if (!rows || !rows.length) {
    table.innerHTML = "<tr><td>No data yet.</td></tr>";
    return;
  }
  const lowered = filterText.trim().toLowerCase();
  const filteredRows = rows
    .slice()
    .filter((row) => {
      if (!lowered) return true;
      return fields.some((field) => String(row[field] ?? "").toLowerCase().includes(lowered));
    });
  if (!filteredRows.length) {
    table.innerHTML = "<tr><td>No rows match the current filter.</td></tr>";
    return;
  }
  const thead = `<thead><tr>${fields.map((field) => `<th>${escapeHtml(field)}</th>`).join("")}</tr></thead>`;
  const tbody = `<tbody>${filteredRows
    .map((row) => `<tr>${fields.map((field) => `<td>${escapeHtml(row[field] ?? "-")}</td>`).join("")}</tr>`)
    .join("")}</tbody>`;
  table.innerHTML = thead + tbody;
}

function parseLaneDisplay(label) {
  const parts = String(label || "").split(" / ");
  if (!parts.length) {
    return { instrument: "-", family: "-" };
  }
  return {
    instrument: parts[0] || "-",
    family: parts.slice(1).join(" / ") || "-",
  };
}

function laneInstrument(row) {
  const lane = parseLaneDisplay(row?.display_name || row?.branch || row?.symbol || "-");
  return row?.symbol || row?.instrument || lane.instrument || "-";
}

function sessionTagMarkup(session) {
  const normalized = String(session || "-").toUpperCase();
  const tone = normalized === "US_LATE" ? "warning" : normalized === "ASIA_EARLY" ? "info" : "muted";
  return `<span class="session-tag ${badgeClass(tone)}">${escapeHtml(normalized || "-")}</span>`;
}

function laneActivityVerdictLevel(verdict) {
  switch (verdict) {
    case "FILLED_OPEN":
      return "warning";
    case "FILLED_CLOSED":
      return "accent";
    case "INTENT_OPEN":
      return "info";
    case "BLOCKED":
    case "HALTED_BY_RISK":
      return "danger";
    case "SIGNAL_ONLY":
      return "info";
    case "NO_ACTIVITY_YET":
      return "muted";
    default:
      return "muted";
  }
}

function renderAdmittedLaneScopeList(targetId, rows) {
  const target = document.getElementById(targetId);
  if (!target) return;
  if (!Array.isArray(rows) || !rows.length) {
    target.innerHTML = "<li>No admitted lane scope available.</li>";
    return;
  }
  target.innerHTML = rows
    .map((row) => {
      const lane = parseLaneDisplay(row.display_name || row.branch || row.symbol || "-");
      return `
        <li class="lane-scope-row">
          <span class="lane-scope-symbol mono">${escapeHtml(laneInstrument(row))}</span>
          <span class="lane-scope-divider">|</span>
          <span class="lane-scope-family mono">${escapeHtml(row.source_family || lane.family || "-")}</span>
          <span class="lane-scope-divider">|</span>
          ${sessionTagMarkup(row.session_restriction)}
        </li>
      `;
    })
    .join("");
}

function renderReviewCard(prefix, payload) {
  const summary = payload?.summary;
  const available = Boolean(payload?.available && summary);
  const assertions = summary?.session_end_assertions || {};
  const flatOk = Boolean(summary?.flat_at_end ?? assertions.flat_at_end);
  const reconOk = Boolean(summary?.reconciliation_clean ?? assertions.reconciliation_clean);
  const unresolved = summary?.unresolved_open_intents ?? 0;
  let label = "NONE";
  let level = "muted";
  if (available) {
    if (!flatOk || !reconOk || unresolved > 0) {
      label = "WARNING";
      level = "danger";
    } else {
      label = "READY";
      level = "ok";
    }
  }
  setBadge(`${prefix}-summary-state`, label, level);
  if (!available) {
    text(`${prefix}-summary-meta`, "No summary bundle has been generated yet.");
  } else {
    text(
      `${prefix}-summary-meta`,
      [
        `Session date: ${summary.session_date || "-"}`,
        `Health: ${summary.health_status || "-"}`,
        `Flat at end: ${flatOk ? "YES" : "NO"}`,
        `Reconciliation clean: ${reconOk ? "YES" : "NO"}`,
        `Unresolved open intents: ${unresolved}`,
        `Closed trades: ${summary.closed_trade_count ?? "-"}`,
        `Realized net P/L: ${summary.realized_net_pnl ?? "-"}`,
        `Summary path: ${summary.summary_path || "-"}`,
      ].join("\n"),
    );
  }
  setLink(`${prefix}-summary-json`, available ? payload.links.json : null);
  setLink(`${prefix}-summary-md`, available ? payload.links.md : null);
  setLink(`${prefix}-summary-blotter`, available ? payload.links.blotter : null);
}

function renderActionLog(rows) {
  const target = document.getElementById("action-log");
  target.innerHTML = "";
  if (!rows.length) {
    target.innerHTML = "<li>No action history yet.</li>";
    return;
  }
  rows
    .slice()
    .reverse()
    .forEach((row) => {
      const item = document.createElement("li");
      item.textContent = `${row.timestamp || "-"} • ${row.action_label || row.action || "-"} • ${row.kind || "-"} • ${row.message || row.output || "-"}`;
      target.appendChild(item);
    });
}

function renderManualControls(controls) {
  const notes = [];
  controls.forEach((control) => {
    const buttonId = controlButtonId(control.label);
    const button = document.getElementById(buttonId);
    if (!button) return;
    button.disabled = !control.enabled || state.actionInFlight;
    if (control.action) {
      button.dataset.action = control.action;
    } else {
      button.removeAttribute("data-action");
    }
    if (!control.enabled && control.reason) {
      notes.push(`${control.label}: ${control.reason}`);
    }
  });
  text("manual-notes", notes.join(" "));
}

function renderPaperPerformance(payload) {
  setBadge("paper-performance-scope", payload.scope_label || "NO SESSION DATA", payload.realized_pnl || payload.unrealized_pnl ? "info" : "muted");
  setLink("paper-performance-link", "/api/operator-artifact/paper-performance");
  setPnlValue("performance-realized", payload.realized_pnl, "N/A");
  setPnlValue("performance-unrealized", payload.unrealized_pnl, "N/A");
  setPnlValue("performance-total", payload.total_pnl, "N/A");
  text("performance-realized-scope", payload.realized_scope || "-");
  text("performance-unrealized-scope", payload.unrealized_scope || "-");
  text("performance-total-scope", payload.total_scope || "-");
  text("performance-exposure", payload.open_exposure_summary || "-");
  text("performance-exposure-source", payload.open_exposure_provenance || "-");
  text("performance-trade-fill", `Trades ${payload.trade_count ?? "-"} | Fills ${payload.fill_count ?? "-"}`);
  text(
    "performance-trade-fill-source",
    `${payload.trade_count_provenance || "-"} ${payload.fill_count_provenance || "-"}`.trim(),
  );
  text(
    "performance-win-loss",
    `Wins ${payload.win_count ?? "-"} | Losses ${payload.loss_count ?? "-"} | Flat ${payload.flat_trade_count ?? "-"}`,
  );
  text("performance-win-loss-source", payload.win_loss_provenance || "-");
  text("performance-realized-source", payload.realized_provenance || "-");
  text("performance-unrealized-source", payload.unrealized_provenance || "-");
  text("performance-total-source", payload.total_provenance || "-");

  const metrics = payload.session_metrics || {};
  text("metric-bars", metrics.processed_bars ?? "-");
  text("metric-signals", metrics.signals_generated ?? "-");
  text("metric-blocked", metrics.blocked_decisions ?? "-");
  text("metric-fills", metrics.fills ?? "-");
  text("metric-exits", metrics.exits ?? "-");
  text("metric-open-closed", `${metrics.open_trade_count ?? "-"} / ${metrics.closed_trade_count ?? "-"}`);
  text("metric-average-trade", metrics.average_realized_per_trade ?? "N/A");
  text("metric-largest-win", metrics.largest_win ?? "N/A");
  text("metric-largest-loss", metrics.largest_loss ?? "N/A");
  text("metric-scope", metrics.scope || "-");
  text("branch-performance-note", payload.branch_provenance || "-");
  text("recent-trades-note", payload.recent_trades_provenance || "-");
}

function renderPaperHistory(payload) {
  const comparison = payload.comparison || {};
  const distribution = payload.distribution || {};
  const drawdown = payload.drawdown || {};
  const latestSession = (payload.recent_sessions || [])[0] || null;
  setBadge("history-scope-badge", payload.history_scope || "NO HISTORY", latestSession ? "info" : "muted");
  setLink("paper-history-link", "/api/operator-artifact/paper-history");
  setLink("history-latest-summary-link", latestSession?.links?.json || null);
  setLink("history-latest-blotter-link", latestSession?.links?.blotter || null);
  setPnlValue("history-vs-prior", comparison.latest_vs_prior_realized, "N/A");
  setPnlValue("history-vs-average", comparison.latest_vs_recent_average, "N/A");
  text("history-vs-prior-scope", comparison.latest_vs_prior_scope || "-");
  text("history-vs-average-scope", comparison.latest_vs_recent_average_scope || "-");
  text("history-trend", comparison.trend || "-");
  text("history-streak", comparison.streak || "-");
  text("history-win-rate", comparison.recent_win_rate || "N/A");
  text("history-win-rate-scope", comparison.recent_win_rate_scope || "-");
  text("history-average-realized", comparison.average_realized_per_session || "N/A");
  text("history-average-realized-scope", comparison.average_realized_scope || "-");
  text("history-average-trades", comparison.average_trades_per_session || "N/A");
  text("history-average-trades-scope", comparison.average_trades_scope || "-");
  text("history-best-session", distribution.best_session || "N/A");
  text("history-worst-session", distribution.worst_session || "N/A");
  text("history-median-session", distribution.median_realized || "N/A");
  text("history-range-session", distribution.pnl_range || "N/A");
  text("history-dispersion", distribution.dispersion || "N/A");
  text("history-positive-negative", `${distribution.positive_session_rate || "N/A"} / ${distribution.negative_session_rate || "N/A"}`);
  text("history-worst-drawdown", drawdown.worst_drawdown || "N/A");
  text("history-distance-hwm", drawdown.distance_from_high_water || "N/A");
  text("history-negative-run", drawdown.negative_run || "-");
  text("history-distribution-scope", distribution.scope || "-");
  text("history-dispersion-note", distribution.dispersion_note || "-");
  text("history-drawdown-scope", drawdown.scope || "-");
  text("history-sample-note", comparison.sample_size_note || "-");
  text("history-note", payload.provenance?.sessions || "-");
  text("branch-history-note", payload.provenance?.branch_history || "-");
  setBadge("branch-panel-scope", payload.latest_completed_session ? "LATEST + RECENT" : "APPROVED SOURCES", payload.latest_completed_session ? "accent" : "muted");
}

function renderPaperSessionShape(payload) {
  const pointCount = (payload.path_points || []).length;
  const hasPath = pointCount > 0;
  const scopeLabel = payload.current_session
    ? "CURRENT SESSION"
    : payload.session_date
      ? `LATEST ${payload.session_date}`
      : "NO SHAPE DATA";
  setBadge("shape-badge", scopeLabel, hasPath ? "info" : "muted");
  setLink("paper-session-shape-link", hasPath ? "/api/operator-artifact/paper-session-shape" : null);

  text("shape-sparkline", payload.sparkline || "No session path yet.");
  text("shape-label", payload.shape_label || "Mixed / unclear");
  text("shape-session-start", payload.session_start || "-");
  text("shape-first-positive", payload.first_positive_transition || "-");
  text("shape-first-negative", payload.first_negative_transition || "-");
  text("shape-high-time", payload.intraday_high_time || "-");
  text("shape-low-time", payload.intraday_low_time || "-");
  text("shape-max-dd-time", payload.max_intraday_drawdown_time || "-");
  text("shape-close-location", payload.close_location || "-");
  text("shape-final-flatten", payload.final_flatten_time || "-");
  text("shape-scope", payload.scope || "-");
  text("shape-path-source", payload.provenance?.path || "-");
  text("shape-current-source", payload.provenance?.current || "-");
  text("shape-granularity-note", payload.granularity_note || "-");
  setPnlValue("shape-high-pnl", payload.intraday_high_pnl, "N/A");
  setPnlValue("shape-low-pnl", payload.intraday_low_pnl, "N/A");
  setPnlValue("shape-current-pnl", payload.current_or_latest_pnl, "N/A");
  setPnlValue("shape-end-realized", payload.end_realized_pnl, "N/A");
  setPnlValue("shape-max-dd", payload.max_intraday_drawdown, "N/A");
}

function renderBranchSessionContribution(payload) {
  setLink("paper-branch-session-link", payload.rows?.length ? "/api/operator-artifact/paper-session-branch-contribution" : null);
  text("branch-session-scope", payload.scope || "-");
  text("branch-session-realized-source", payload.provenance?.realized || "-");
  text("branch-session-timing-source", payload.provenance?.timing || "-");
  text("branch-session-granularity-note", payload.granularity_note || "-");
  text("branch-performance-note", payload.provenance?.phase || "-");
  renderContributionCard("branch-top-contributor", "branch-top-contributor-note", payload.top_contributor);
  renderContributionCard("branch-top-detractor", "branch-top-detractor-note", payload.top_detractor);
  renderContributionCard("branch-early-run-up", "branch-early-run-up-note", payload.phase_summary?.early_run_up);
  renderContributionCard("branch-early-drawdown", "branch-early-drawdown-note", payload.phase_summary?.early_drawdown);
  renderContributionCard("branch-late-recovery", "branch-late-recovery-note", payload.phase_summary?.late_recovery);
  renderContributionCard("branch-late-fade", "branch-late-fade-note", payload.phase_summary?.late_fade);
}

function renderPaperSessionEventTimeline(payload) {
  const events = payload.events || [];
  setBadge("timeline-scope-badge", events.length ? "LATEST SESSION" : "NO TIMELINE", events.length ? "info" : "muted");
  setLink("paper-session-timeline-link", events.length ? "/api/operator-artifact/paper-session-event-timeline" : null);
  text("timeline-scope", payload.scope || "-");
  text("timeline-shape-source", payload.provenance?.shape || "-");
  text("timeline-branch-source", payload.provenance?.branch || "-");
  text("timeline-operator-source", payload.provenance?.operator || "-");
  text("timeline-granularity-note", payload.granularity_note || "-");

  const target = document.getElementById("session-event-timeline");
  target.innerHTML = "";
  if (!events.length) {
    target.innerHTML = "<li><div class=\"timeline-content\"><div class=\"timeline-title\">No latest-session timeline events yet.</div></div></li>";
    return;
  }

  events.forEach((event) => {
    const item = document.createElement("li");
    const details = (event.details || []).map((detail) => escapeHtml(detail)).join("<br />");
    item.innerHTML = `
      <div class="timeline-time">${escapeHtml(event.timestamp || "-")}</div>
      <div class="timeline-content">
        <div class="timeline-title-row">
          <span class="badge ${timelineBadgeClass(event.category)}">${escapeHtml(event.badge || event.category || "EVENT")}</span>
          <span class="timeline-title">${escapeHtml(event.title || "-")}</span>
        </div>
        <div class="timeline-details">${details || "-"}</div>
        <div class="timeline-provenance">${escapeHtml(event.provenance || "-")}</div>
      </div>
    `;
    target.appendChild(item);
  });
}

function renderPaperModeStatus(payload) {
  setStatusValue("paper-ready-runtime", payload.runtime_phase || (payload.runtime_running ? "RUNNING" : "STOPPED"), payload.runtime_phase === "STOPPING" ? "warning" : payload.runtime_running ? "ok" : "muted");
  setStatusValue("paper-ready-entries", payload.entries_enabled ? "ENABLED" : "HALTED", payload.entries_enabled ? "ok" : "warning");
  text("paper-ready-approved-count", `${payload.approved_models_active ?? 0} / ${payload.approved_models_total ?? 0}`);
  const laneRows = Array.isArray(payload.lane_risk_rows) ? payload.lane_risk_rows : [];
  const instrumentCount = new Set(laneRows.map((row) => laneInstrument(row)).filter(Boolean)).size;
  text("paper-ready-instrument-count", instrumentCount ? String(instrumentCount) : "0");
  text(
    "paper-ready-instrument",
    laneRows.length
      ? `${laneRows.length} lanes across ${instrumentCount || 0} instruments`
      : (payload.instrument_scope || "-"),
  );
  text("paper-ready-last-fill", payload.latest_paper_fill_timestamp || "No paper fill yet");
  text("paper-ready-last-decision", payload.latest_paper_decision_timestamp || "No paper decision yet");
  text("paper-ready-exposure", payload.open_exposure_state || "-");
  setStatusValue("paper-ready-flat-state", payload.flat_state ? "FLAT" : "OPEN", payload.flat_state ? "ok" : "warning");
  setStatusValue("paper-ready-desk-risk-state", payload.desk_risk_state || "OK", paperExceptionVerdictLevel(payload.desk_risk_state || "OK"));
  text("paper-ready-session-realized", payload.session_realized_pnl || "0");
  text("paper-ready-session-unrealized", payload.session_unrealized_pnl || "0");
  text("paper-ready-session-total", payload.session_total_pnl || "0");
  text(
    "paper-ready-desk-thresholds",
    `Halt ${payload.desk_halt_new_entries_loss || "-"} | Flatten ${payload.desk_flatten_and_halt_loss || "-"}`,
  );
  text("paper-ready-desk-unblock", payload.desk_unblock_action || "-");
  text(
    "paper-ready-last-control",
    payload.last_control_action
      ? `${payload.last_control_action} • ${payload.last_control_status || "-"} • ${payload.last_control_timestamp || "-"}`
      : "No paper operator control applied yet",
  );
  text("paper-ready-halt-reason", payload.halt_reason || "-");
  text("paper-ready-desk-risk-reason", payload.desk_risk_reason || "-");
  text(
    "paper-ready-approved-label",
    laneRows.length
      ? `Lane-level truth • ${laneRows.length} admitted lanes • ${instrumentCount || 0} instruments`
      : (payload.approved_models_label || "-"),
  );
  text(
    "paper-ready-lane-eligibility-note",
    `Current runtime session: ${payload.current_detected_session || "UNKNOWN"}`,
  );
  setLink("paper-ready-status-link", payload.artifacts?.status || null);
  setLink("paper-ready-approved-link", payload.artifacts?.approved_models || null);
  setLink("paper-ready-desk-risk-link", payload.artifacts?.desk_risk || null);
  setLink("paper-ready-lane-risk-link", payload.artifacts?.lane_risk || null);
  setLink("paper-ready-risk-events-link", payload.artifacts?.risk_events || null);
  setLink("paper-ready-config-link", payload.artifacts?.config_in_force || null);
  setLink("paper-ready-decisions-link", payload.artifacts?.decisions || null);
  setLink("paper-ready-intents-link", payload.artifacts?.intents || null);
  setLink("paper-ready-fills-link", payload.artifacts?.fills || null);
  setLink("paper-ready-blotter-link", payload.artifacts?.blotter || null);
  setLink("paper-ready-position-link", payload.artifacts?.position || null);
  setLink("paper-ready-blocks-link", payload.artifacts?.blocks || null);
  setLink("paper-ready-reconciliation-link", payload.artifacts?.reconciliation || null);
  setLink("paper-ready-alerts-link", payload.artifacts?.alerts || null);

  renderAdmittedLaneScopeList("paper-ready-admitted-lane-list", laneRows);

  const laneList = document.getElementById("paper-ready-lane-risk-list");
  if (laneList) {
    if (!laneRows.length) {
      laneList.innerHTML = "<li>No persisted lane risk state yet.</li>";
    } else {
      laneList.innerHTML = laneRows
        .map(
          (row) => `
            <li>
              <div class="exception-row-top">
                <span class="badge ${paperExceptionVerdictLevel(row.risk_state || "OK")}">${escapeHtml(row.risk_state || "OK")}</span>
                <span class="timeline-title">${escapeHtml(row.display_name || row.symbol || "-")}</span>
              </div>
              <div class="timeline-details">
                Session ${escapeHtml(row.session_restriction || "-")} | Cat Cap ${escapeHtml(row.catastrophic_open_loss_threshold || "-")} | Losers ${escapeHtml(String(row.realized_losing_trades ?? 0))}
              </div>
              <div class="timeline-provenance">
                ${escapeHtml(row.halt_reason || "No active lane halt")} | ${escapeHtml(row.unblock_action || "No action needed; already eligible")}
              </div>
            </li>
          `,
        )
        .join("");
    }
  }

  const eligibilityRows = Array.isArray(payload.lane_eligibility_rows) ? payload.lane_eligibility_rows : [];
  renderTable(
    "paper-ready-lane-eligibility-table",
    eligibilityRows.map((row) => ({
      lane: row.display_name || row.lane_id || "-",
      symbol: row.symbol || "-",
      allowed_session: row.configured_allowed_sessions || "ANY",
      current_session: row.current_detected_session || payload.current_detected_session || "UNKNOWN",
      eligible_now: row.eligible_now ? "YES" : "NO",
      reason: row.eligibility_reason || (row.eligible_now ? "-" : "unknown"),
    })),
    ["lane", "symbol", "allowed_session", "current_session", "eligible_now", "reason"],
  );
}

function renderPaperEntryEligibility(payload) {
  setBadge("paper-entry-eligibility-badge", payload.verdict || "UNKNOWN / INSUFFICIENT STATE", paperEntryEligibilityLevel(payload.verdict));
  setStatusValue("paper-entry-eligibility-verdict", payload.verdict || "UNKNOWN / INSUFFICIENT STATE", paperEntryEligibilityLevel(payload.verdict));
  setStatusValue("paper-entry-eligibility-action", payload.clear_action || "Manual inspection required", paperEntryActionLevel(payload.clear_action));
  setStatusValue(
    "paper-entry-eligibility-fireability",
    payload.approved_models_eligible_now ? "YES" : "NO",
    payload.approved_models_eligible_now ? "ok" : "warning",
  );
  text("paper-entry-eligibility-note", payload.state_note || "-");
  text("paper-entry-eligibility-provenance", payload.provenance || "-");

  const reasons = Array.isArray(payload.reasons) ? payload.reasons : [];
  const list = document.getElementById("paper-entry-eligibility-reasons");
  if (!reasons.length) {
    list.innerHTML = "<li>No persisted eligibility reasons are available yet.</li>";
    return;
  }
  list.innerHTML = reasons
    .map(
      (item) => `
        <li>
          <div class="exception-row-top">
            <div class="timeline-title-row">
              <span class="timeline-title">${escapeHtml(item.label || "-")}</span>
              <span class="subnote mono">${escapeHtml(item.value || "-")}</span>
            </div>
            <span class="timeline-time">${escapeHtml(item.timestamp || "-")}</span>
          </div>
          <div class="model-event-details">Source: ${escapeHtml(item.source || "-")}</div>
        </li>
      `,
    )
    .join("");
}

function renderPaperActivityProof(payload) {
  const summary = payload.session_summary || {};
  setBadge("paper-activity-proof-verdict", payload.verdict || "INSUFFICIENT EVIDENCE", paperActivityProofLevel(payload.verdict));
  setStatusValue("paper-activity-runtime", summary.polling_runtime_active ? "YES" : "NO", summary.polling_runtime_active ? "ok" : "warning");
  text("paper-activity-bars", summary.bars_processed_count ?? "Unavailable");
  text("paper-activity-seen", summary.approved_models_seen_count ?? 0);
  text("paper-activity-signals", summary.total_signals_count ?? 0);
  text("paper-activity-blocked", summary.total_blocked_count ?? 0);
  text("paper-activity-decisions", summary.total_decisions_count ?? 0);
  text("paper-activity-intents", summary.total_intents_count ?? 0);
  text("paper-activity-fills", summary.total_fills_count ?? 0);
  text("paper-activity-latest-event", summary.latest_approved_model_event_timestamp || "No approved-model event yet");
  text("paper-activity-note", payload.no_trade_note || "-");
  text("paper-activity-proof-provenance", payload.provenance || "-");
  const stale = document.getElementById("paper-activity-stale-watch");
  if (stale) {
    if (payload.stale_watch && payload.stale_watch_message) {
      stale.classList.remove("hidden");
      stale.textContent = payload.stale_watch_message;
    } else {
      stale.classList.add("hidden");
      stale.textContent = "";
    }
  }
  renderTable(
    "paper-activity-proof-table",
    (payload.per_model_rows || []).map((row) => ({
      branch: row.branch,
      armed: row.armed ? "YES" : "NO",
      latest_activity: row.latest_activity_type || "NO_ACTIVITY",
      latest_timestamp: row.latest_activity_timestamp || "No activity yet",
      signals: row.signals ?? 0,
      blocks: row.blocks ?? 0,
      decisions: row.decisions ?? 0,
      intents: row.intents ?? 0,
      fills: row.fills ?? 0,
    })),
    ["branch", "armed", "latest_activity", "latest_timestamp", "signals", "blocks", "decisions", "intents", "fills"],
  );
}

function renderPaperExceptions(payload) {
  const summary = payload.summary || {};
  setBadge("paper-exception-verdict", payload.session_verdict || "UNKNOWN", paperExceptionVerdictLevel(payload.session_verdict));
  setLink("paper-exception-snapshot-link", payload.artifacts?.snapshot || null);
  setStatusValue("paper-exception-open", summary.open_exposure ? "YES" : "NO", summary.open_exposure ? "warning" : "ok");
  text("paper-exception-owner", summary.owning_model || (summary.open_exposure ? "UNKNOWN / INSUFFICIENT_ARTIFACTS" : "NONE"));
  text(
    "paper-exception-open-meta",
    summary.open_exposure
      ? `${summary.open_qty ?? 0} @ ${summary.open_average_price || "-"}`
      : "No open paper exposure"
  );
  text("paper-exception-unresolved", String(summary.unresolved_intents ?? 0));
  setStatusValue(
    "paper-exception-reconciliation",
    summary.reconciliation_state || "UNKNOWN",
    summary.reconciliation_state === "CLEAN" ? "ok" : summary.reconciliation_state ? "danger" : "muted",
  );
  setStatusValue("paper-exception-entries", summary.entries_state || "-", summary.entries_state === "ENABLED" ? "ok" : "warning");
  setStatusValue("paper-exception-flatten", summary.flatten_pending ? "PENDING" : "IDLE", summary.flatten_pending ? "warning" : "muted");
  setStatusValue(
    "paper-exception-stop-after-cycle",
    summary.stop_after_cycle_pending ? "PENDING" : "IDLE",
    summary.stop_after_cycle_pending ? "warning" : "muted",
  );
  text("paper-exception-note", payload.verdict_note || summary.position_owner_note || "-");

  const list = document.getElementById("paper-exceptions-list");
  const exceptions = Array.isArray(payload.exceptions) ? payload.exceptions : [];
  if (!exceptions.length) {
    list.innerHTML = "<li>No active paper exceptions from current persisted artifacts.</li>";
    return;
  }
  list.innerHTML = exceptions
    .map((item) => {
      const focusButton = item.model_branch
        ? `<button class="link-button subtle-link exception-focus-button" data-branch="${escapeHtml(item.model_branch)}">Focus Model</button>`
        : "";
      const artifactLink = item.artifact_href
        ? `<a class="link-button subtle-link" href="${escapeHtml(item.artifact_href)}" target="_blank" rel="noopener noreferrer">${escapeHtml(item.artifact_label || "Artifact")}</a>`
        : "";
      return `
        <li>
          <div class="exception-row-top">
            <div class="timeline-title-row">
              <span class="${badgeClass(paperExceptionSeverityLevel(item.severity))}">${escapeHtml(item.severity || "INFO")}</span>
              <span class="timeline-title">${escapeHtml(item.code || "-")}</span>
              ${item.model_branch ? `<span class="subnote mono">${escapeHtml(item.model_branch)}</span>` : ""}
            </div>
            <span class="timeline-time">${escapeHtml(item.timestamp || "-")}</span>
          </div>
          <div class="model-event-details">${escapeHtml(item.details || "-")}</div>
          <div class="exception-recommendation">Recommended: ${escapeHtml(item.recommendation || "-")}</div>
          <div class="model-event-links">${focusButton}${artifactLink}</div>
        </li>
      `;
    })
    .join("");

  list.querySelectorAll(".exception-focus-button").forEach((button) => {
    button.addEventListener("click", () => {
      state.selectedApprovedModel = button.dataset.branch || null;
      render();
    });
  });
}

function renderLaneRegistrySections(payload) {
  const sections = Array.isArray(payload.sections) ? payload.sections : [];
  const sectionMap = new Map(sections.map((section) => [section.key, section]));
  document.querySelectorAll("[data-lane-section]").forEach((element) => {
    const sectionKey = element.dataset.laneSection;
    renderLaneRegistrySection(element, sectionMap.get(sectionKey) || { key: sectionKey, rows: [] });
  });
}

function renderLaneRegistrySection(element, section) {
  if (!element) return;
  const titleNode = element.querySelector("[data-role='title']");
  const eyebrowNode = element.querySelector("[data-role='eyebrow']");
  const badgeNode = element.querySelector("[data-role='badge']");
  const summaryNode = element.querySelector("[data-role='summary']");
  const metricsNode = element.querySelector("[data-role='summary-metrics']");
  const listNode = element.querySelector("[data-role='card-list']");
  const primaryLink = element.querySelector("[data-role='primary-link']");
  const secondaryLink = element.querySelector("[data-role='secondary-link']");
  const rows = Array.isArray(section.rows) ? section.rows : [];

  if (titleNode && section.title) titleNode.textContent = section.title;
  if (eyebrowNode && section.eyebrow) eyebrowNode.textContent = section.eyebrow;
  if (badgeNode) {
    badgeNode.textContent = section.badge_label || "NO LANES";
    badgeNode.className = badgeClass(section.badge_level || "muted");
  }
  if (summaryNode) summaryNode.textContent = section.summary_line || "No surfaced lanes in this section.";
  renderLaneSurfaceLink(primaryLink, section.primary_link || {});
  renderLaneSurfaceLink(secondaryLink, section.secondary_link || {});

  if (metricsNode) {
    const metrics = Array.isArray(section.summary_metrics) ? section.summary_metrics : [];
    metricsNode.innerHTML = metrics.length
      ? metrics
          .map(
            (metric) => `
              <div class="stat">
                <span class="label">${escapeHtml(metric.label || "-")}</span>
                <span class="value mono">${escapeHtml(metric.value ?? "-")}</span>
              </div>
            `,
          )
          .join("")
      : `<div class="stat wide"><span class="label">Status</span><span class="value mono">No section metrics available.</span></div>`;
  }

  if (!listNode) return;
  if (!rows.length) {
    listNode.innerHTML = `<article class="lane-surface-card"><div class="subnote">No surfaced lanes are available in this section.</div></article>`;
    return;
  }
  listNode.innerHTML = rows
    .slice()
    .sort((left, right) => (left.display_priority ?? 0) - (right.display_priority ?? 0))
    .map((row) => renderLaneSurfaceCard(row))
    .join("");
}

function renderLaneSurfaceLink(anchor, linkConfig) {
  if (!anchor) return;
  const href = linkConfig?.href || null;
  const defaultLabel = anchor.dataset.defaultLabel || anchor.textContent || "Artifact";
  anchor.textContent = linkConfig?.label || defaultLabel;
  if (href) {
    anchor.href = href;
    anchor.classList.remove("disabled");
  } else {
    anchor.href = "#";
    anchor.classList.add("disabled");
  }
}

function renderLaneSurfaceCard(row) {
  const warnings = Array.isArray(row.warnings) ? row.warnings : [];
  const summaryLines = Array.isArray(row.summary_lines) ? row.summary_lines : [];
  const metrics = Array.isArray(row.card_metrics) ? row.card_metrics : [];
  const primaryBadge = row.primary_badge || {};
  return `
    <article class="lane-surface-card surface-${escapeHtml(row.surface_group || "unknown")}">
      <div class="lane-surface-card-top">
        <div>
          <div class="lane-surface-card-title">${escapeHtml(row.display_name || row.lane_id || "-")}</div>
          <div class="lane-surface-card-subtitle mono">${escapeHtml(row.lane_id || "-")}</div>
        </div>
        <span class="${badgeClass(primaryBadge.level || "muted")}">${escapeHtml(primaryBadge.label || "UNKNOWN")}</span>
      </div>
      <div class="lane-surface-chip-row">
        ${metrics.map((metric) => `<span class="lane-surface-chip"><strong>${escapeHtml(metric.label || "-")}</strong> ${escapeHtml(metric.value ?? "-")}</span>`).join("")}
      </div>
      <div class="lane-surface-summary-block">
        <div class="lane-surface-summary-label">Scope</div>
        <div class="lane-surface-summary-list">
          <span>${escapeHtml(row.scope_summary || "-")}</span>
          <span>${escapeHtml(row.family || row.classification || "-")}</span>
        </div>
      </div>
      <div class="lane-surface-summary-block">
        <div class="lane-surface-summary-label">Monitoring</div>
        <div class="lane-surface-summary-list">
          <span>${escapeHtml(row.monitoring_summary || "-")}</span>
          <span>${escapeHtml(`exit=${row.active_exit || "-"}`)}</span>
        </div>
      </div>
      <div class="lane-surface-summary-block">
        <div class="lane-surface-summary-label">Summary</div>
        <div class="lane-surface-summary-list">
          ${summaryLines.length ? summaryLines.map((line) => `<span>${escapeHtml(line)}</span>`).join("") : "<span>No summary available.</span>"}
        </div>
      </div>
      <div class="lane-surface-card-subtitle ${warnings.length ? "lane-surface-warning" : ""}">${escapeHtml(row.warning_summary || "No warnings.")}</div>
    </article>
  `;
}

function lenOrZero(value) {
  return Array.isArray(value) ? value.length : 0;
}

function renderApprovedModels(payload, eligibility = {}, temporaryPayload = {}) {
  const approvedRows = Array.isArray(payload.rows) ? payload.rows : [];
  const temporaryRows = Array.isArray(temporaryPayload.rows) ? temporaryPayload.rows.map(normalizeTemporaryPaperStrategyForRoster) : [];
  const rows = [...approvedRows, ...temporaryRows];
  const detailsByBranch = {
    ...(payload.details_by_branch || {}),
    ...Object.fromEntries(
      (Array.isArray(temporaryPayload.rows) ? temporaryPayload.rows : []).map((row) => [
        String(row.display_name || row.branch || row.lane_id || "-"),
        normalizeTemporaryPaperStrategyDetail(row, temporaryPayload.artifacts || {}),
      ]),
    ),
  };
  const enabled = rows.filter((row) => row.enabled);
  const longEnabled = enabled.filter((row) => row.side === "LONG").length;
  const shortEnabled = enabled.filter((row) => row.side === "SHORT").length;
  const temporaryCount = rows.filter((row) => row.temporary_paper_strategy).length;
  const instrumentCount = new Set(rows.map((row) => laneInstrument(row)).filter(Boolean)).size;
  setBadge(
    "approved-models-badge",
    `${enabled.length}/${rows.length} ACTIVE LANES`,
    enabled.length ? "accent" : "muted",
  );
  text(
    "approved-models-scope",
    rows.length
      ? `Lane-level truth • ${rows.length} paper strategies • ${instrumentCount || 0} instruments`
      : (payload.instrument_scope || payload.scope_label || "-"),
  );
  text("approved-models-enabled", `${enabled.length} / ${rows.length}`);
  text("approved-models-long", `${longEnabled}`);
  text("approved-models-short", `${shortEnabled}`);
  text(
    "approved-models-out-of-scope-note",
    temporaryCount
      ? `${payload.out_of_scope_note || "-"} Temporary paper strategies are shown here with explicit experimental/paper-only/non-approved labels.`
      : (payload.out_of_scope_note || "-"),
  );
  const fireabilityState = document.getElementById("approved-models-fireability-state");
  const fireabilityDetail = document.getElementById("approved-models-fireability-detail");
  const fireabilityJump = document.getElementById("approved-models-fireability-jump");
  if (fireabilityState) {
    fireabilityState.textContent = eligibility.approved_models_eligible_now ? "YES" : "NO";
    fireabilityState.className = `value status-${eligibility.approved_models_eligible_now ? "ok" : "warning"} mono`;
  }
  if (fireabilityDetail) {
    if (eligibility.approved_models_eligible_now) {
      fireabilityDetail.textContent = eligibility.signal_seen_this_session ? "Eligible now; signal presence depends on current model conditions." : "Eligible now. No active signal yet.";
    } else {
      fireabilityDetail.textContent = eligibility.primary_blocking_reason
        ? `${eligibility.primary_blocking_reason.replaceAll("_", " ")}`
        : "See PAPER ENTRY ELIGIBILITY.";
    }
  }
  if (fireabilityJump) {
    fireabilityJump.classList.toggle("hidden", Boolean(eligibility.approved_models_eligible_now));
  }
  setLink("paper-approved-models-link", payload.artifacts?.approved_models || null);
  setLink("paper-approved-models-decisions-link", payload.artifacts?.decisions || null);
  setLink("paper-approved-models-intents-link", payload.artifacts?.intents || null);
  setLink("paper-approved-models-fills-link", payload.artifacts?.fills || null);
  text("approved-models-eligibility-source", payload.provenance?.eligibility || "-");
  text("approved-models-signal-source", payload.provenance?.last_signal || "-");
  text("approved-models-intent-source", payload.provenance?.last_intent || "-");
  text("approved-models-fill-source", payload.provenance?.last_fill || "-");
  text("approved-models-pnl-source", payload.provenance?.realized || "-");

  const table = document.getElementById("approved-models-table");
  if (table) {
    table.className = "approved-models-table-compact";
    if (!rows.length) {
      table.innerHTML = "<tr><td>No paper strategies are active yet.</td></tr>";
    } else {
      table.innerHTML = `
        <thead>
          <tr>
            <th>instrument</th>
            <th>family</th>
            <th>session</th>
            <th>class</th>
            <th>state</th>
            <th>side</th>
            <th>signals</th>
            <th>blocked</th>
            <th>intents</th>
            <th>fills</th>
            <th>open</th>
            <th>chain</th>
            <th>realized_pnl</th>
            <th>unrealized_pnl</th>
          </tr>
        </thead>
        <tbody>
          ${rows
            .map((row) => {
              const lane = parseLaneDisplay(row.branch);
              return `
                <tr>
                  <td class="approved-lane-instrument mono">${escapeHtml(laneInstrument(row))}</td>
                  <td class="approved-lane-family">
                    <div class="approved-lane-family-wrap">
                      <span class="approved-lane-family-name mono">${escapeHtml(row.source_family || lane.family || "-")}</span>
                      <span class="approved-lane-label subnote mono">${escapeHtml(row.branch || "-")}</span>
                    </div>
                  </td>
                  <td>${sessionTagMarkup(row.session_restriction)}</td>
                  <td>
                    <div class="approved-lane-family-wrap">
                      <span class="${row.temporary_paper_strategy ? "badge badge-warning" : "badge badge-accent"}">${escapeHtml(row.temporary_paper_strategy ? "TEMP PAPER" : "ADMITTED")}</span>
                      ${row.temporary_paper_strategy ? `
                        <span class="approved-lane-label subnote mono">EXPERIMENTAL</span>
                        <span class="approved-lane-label subnote mono">PAPER ONLY</span>
                        <span class="approved-lane-label subnote mono">NON-APPROVED</span>
                      ` : ""}
                    </div>
                  </td>
                  <td><span class="${row.enabled ? "badge badge-accent" : "badge badge-muted"}">${escapeHtml(row.state || "-")}</span></td>
                  <td>${escapeHtml(row.side || "-")}</td>
                  <td>${escapeHtml(row.signal_count ?? 0)}</td>
                  <td>${escapeHtml(row.blocked_count ?? 0)}</td>
                  <td>${escapeHtml(row.intent_count ?? 0)}</td>
                  <td>${escapeHtml(row.fill_count ?? 0)}</td>
                  <td>${escapeHtml(row.open_position ? "YES" : "NO")}</td>
                  <td><span class="${badgeClass(row.temporary_paper_strategy ? "warning" : approvedModelChainLevel(row.chain_state || "UNKNOWN"))}">${escapeHtml(row.chain_state || "UNKNOWN")}</span></td>
                  <td>${escapeHtml(row.realized_pnl ?? "N/A")}</td>
                  <td>${escapeHtml(row.unrealized_pnl ?? "N/A")}</td>
                </tr>
              `;
            })
            .join("")}
        </tbody>
      `;
    }
  }

  const select = document.getElementById("approved-model-select");
  const availableBranches = rows.map((row) => row.branch);
  if (select) {
    const currentSelection = availableBranches.includes(state.selectedApprovedModel)
      ? state.selectedApprovedModel
      : (payload.default_branch && availableBranches.includes(payload.default_branch) ? payload.default_branch : availableBranches[0] || null);
    state.selectedApprovedModel = currentSelection;
    select.innerHTML = availableBranches
      .map((branch) => {
        const row = rows.find((candidate) => candidate.branch === branch) || {};
        const lane = parseLaneDisplay(branch);
        const instrument = laneInstrument(row);
        const family = row.source_family || lane.family || branch;
        const session = row.session_restriction || "-";
        return `<option value="${escapeHtml(branch)}">${escapeHtml(`${instrument} | ${family} | ${session}`)}</option>`;
      })
      .join("");
    if (currentSelection) {
      select.value = currentSelection;
    }
  }

  renderApprovedModelDetail(detailsByBranch[state.selectedApprovedModel] || null, payload.artifacts || {});
}

function renderTemporaryPaperStrategies(payload) {
  const rows = Array.isArray(payload.rows) ? payload.rows : [];
  setLink("temporary-paper-strategies-link", payload.artifacts?.snapshot || null);
  text("temporary-paper-strategies-count", String(payload.total_count ?? rows.length));
  text("temporary-paper-strategies-enabled", String(payload.enabled_count ?? 0));
  text("temporary-paper-strategies-disabled", String(payload.disabled_count ?? 0));
  text("temporary-paper-strategies-signals", String(payload.recent_signal_count ?? 0));
  text("temporary-paper-strategies-events", String(payload.recent_event_count ?? 0));
  text("temporary-paper-strategies-kill-switch", payload.kill_switch_active ? "ACTIVE" : "INACTIVE");
  text("temporary-paper-strategies-note", payload.note || payload.scope_label || "No temporary paper strategies are visible.");
  text("temporary-paper-strategies-metrics-bucket", payload.metrics_bucket || "-");

  const table = document.getElementById("temporary-paper-strategies-table");
  if (!table) return;
  table.className = "approved-models-table-compact";
  if (!rows.length) {
    table.innerHTML = "<tr><td>No temporary paper strategies are active.</td></tr>";
    return;
  }
  table.innerHTML = `
    <thead>
      <tr>
        <th>instrument</th>
        <th>lane</th>
        <th>status</th>
        <th>guardrails</th>
        <th>side</th>
        <th>signals</th>
        <th>events</th>
        <th>allow/block/override</th>
        <th>last update</th>
      </tr>
    </thead>
    <tbody>
      ${rows
        .map((row) => `
          <tr>
            <td class="approved-lane-instrument mono">${escapeHtml(row.instrument || "-")}</td>
            <td class="approved-lane-family">
              <div class="approved-lane-family-wrap">
                <span class="approved-lane-family-name mono">${escapeHtml(row.display_name || row.branch || row.lane_id || "-")}</span>
                <span class="approved-lane-label subnote mono">${escapeHtml(row.lane_id || "-")}</span>
              </div>
            </td>
            <td>
              <div class="approved-lane-family-wrap">
                <span class="${badgeClass(row.kill_switch_active ? "danger" : (String(row.state || "").toUpperCase() === "ENABLED" ? "ok" : "warning"))}">${escapeHtml(row.state || "-")}</span>
                <span class="approved-lane-label subnote mono">${escapeHtml(row.kill_switch_active ? "KILL SWITCH ACTIVE" : "LOWER PRIORITY")}</span>
              </div>
            </td>
            <td>
              <div class="approved-lane-family-wrap">
                <span class="badge badge-warning">EXPERIMENTAL</span>
                <span class="approved-lane-label subnote mono">${escapeHtml(row.paper_only ? "PAPER ONLY" : "-")}</span>
                <span class="approved-lane-label subnote mono">${escapeHtml(row.non_approved ? "NON-APPROVED" : "-")}</span>
                <span class="approved-lane-label subnote mono">${escapeHtml(row.quality_bucket_policy || "-")}</span>
              </div>
            </td>
            <td>${escapeHtml(row.side || row.position_side || "-")}</td>
            <td>${escapeHtml(String(row.recent_signal_count ?? row.signal_count ?? 0))}</td>
            <td>${escapeHtml(String(row.recent_event_count ?? row.event_count ?? 0))}</td>
            <td>${escapeHtml(row.allow_block_override_summary?.label || row.latest_signal_label || "-")}</td>
            <td>${escapeHtml(row.last_update_timestamp || row.latest_activity_timestamp || "-")}</td>
          </tr>
        `)
        .join("")}
    </tbody>
  `;
}

function renderSummaryChips(values) {
  const rows = Array.isArray(values) ? values : [];
  if (!rows.length) {
    return '<span class="subnote">No attribution summary available yet.</span>';
  }
  return rows.map((value) => `<span class="lane-surface-chip">${escapeHtml(value)}</span>`).join("");
}

function renderTrackedPaperStrategies(payload) {
  const rows = Array.isArray(payload.rows) ? payload.rows : [];
  const defaultStrategyId = payload.default_strategy_id || (rows[0] && rows[0].strategy_id) || null;
  const detail = (payload.details_by_strategy_id || {})[defaultStrategyId] || rows[0] || {};
  text("tracked-paper-strategies-count", String(payload.total_count ?? rows.length));
  text("tracked-paper-strategies-enabled", String(payload.enabled_count ?? rows.filter((row) => row.enabled).length));
  text("tracked-paper-strategies-active", String(payload.active_count ?? rows.filter((row) => ["READY", "IN_POSITION", "RECONCILING"].includes(String(row.status || "").toUpperCase())).length));
  text("tracked-paper-strategy-label", detail.internal_label || "-");
  text("tracked-paper-strategy-status", detail.status || "DISABLED");
  text("tracked-paper-strategy-session", detail.current_session_segment || "-");
  text("tracked-paper-strategies-note", payload.note || payload.scope_label || "No tracked paper strategies are registered.");
  setLink("tracked-paper-strategies-link", "/api/operator-artifact/paper-tracked-strategies");
  setLink("tracked-paper-strategy-details-link", "/api/operator-artifact/paper-tracked-strategy-details");

  const table = document.getElementById("tracked-paper-strategies-table");
  if (table) {
    table.className = "approved-models-table-compact";
    if (!rows.length) {
      table.innerHTML = "<tr><td>No tracked paper strategies are registered.</td></tr>";
    } else {
      table.innerHTML = `
        <thead>
          <tr>
            <th>strategy</th>
            <th>env</th>
            <th>status</th>
            <th>enabled</th>
            <th>session</th>
            <th>side</th>
            <th>realized</th>
            <th>open</th>
            <th>last update</th>
          </tr>
        </thead>
        <tbody>
          ${rows.map((row) => `
            <tr>
              <td class="approved-lane-family">
                <div class="approved-lane-family-wrap">
                  <span class="approved-lane-family-name">${escapeHtml(row.display_name || row.strategy_id || "-")}</span>
                  <span class="approved-lane-label subnote mono">${escapeHtml(row.internal_label || row.strategy_id || "-")}</span>
                </div>
              </td>
              <td>${escapeHtml(row.environment || "-")}</td>
              <td><span class="${badgeClass(String(row.status || "").toUpperCase() === "FAULT" ? "danger" : (String(row.status || "").toUpperCase() === "IN_POSITION" ? "accent" : (String(row.status || "").toUpperCase() === "READY" ? "ok" : "muted")))}">${escapeHtml(row.status || "-")}</span></td>
              <td>${escapeHtml(row.enabled ? "YES" : "NO")}</td>
              <td>${escapeHtml(row.current_session_segment || "-")}</td>
              <td>${escapeHtml(row.current_position_side || "-")}</td>
              <td>${escapeHtml(row.realized_pnl || "-")}</td>
              <td>${escapeHtml(row.open_pnl || "-")}</td>
              <td>${escapeHtml(row.last_update_timestamp || "-")}</td>
            </tr>
          `).join("")}
        </tbody>
      `;
    }
  }

  text("tracked-paper-detail-name", detail.display_name || "-");
  text("tracked-paper-detail-env", detail.environment || "-");
  text("tracked-paper-detail-runtime-attached", detail.runtime_attached ? "ATTACHED" : "DETACHED");
  text(
    "tracked-paper-detail-heartbeat-age",
    detail.runtime_heartbeat_age_seconds == null ? "-" : `${Math.round(detail.runtime_heartbeat_age_seconds)}s`,
  );
  text("tracked-paper-detail-data-stale", detail.data_stale ? "YES" : "NO");
  text("tracked-paper-detail-entries", detail.entries_enabled ? "ENABLED" : "DISABLED");
  text("tracked-paper-detail-halt", detail.operator_halt ? "HALTED" : "CLEAR");
  text("tracked-paper-detail-warmup", detail.warmup_complete == null ? "UNKNOWN" : (detail.warmup_complete ? "COMPLETE" : "INCOMPLETE"));
  text("tracked-paper-detail-position", detail.current_position_side || "-");
  text("tracked-paper-detail-qty", detail.current_quantity == null ? "-" : String(detail.current_quantity));
  text("tracked-paper-detail-family", detail.current_entry_family || "-");
  text("tracked-paper-detail-bars-in-trade", detail.bars_in_trade == null ? "-" : String(detail.bars_in_trade));
  text("tracked-paper-detail-bar-ts", detail.latest_processed_bar_timestamp || "-");
  setPnlValue("tracked-paper-detail-realized", detail.realized_pnl, "-");
  setPnlValue("tracked-paper-detail-open-pnl", detail.open_pnl, "N/A");
  text("tracked-paper-detail-win-rate", detail.win_rate ? `${detail.win_rate}%` : "-");
  text("tracked-paper-detail-profit-factor", detail.profit_factor || "-");
  text("tracked-paper-detail-max-drawdown", detail.max_drawdown || "-");
  setPnlValue("tracked-paper-detail-day-pnl", detail.current_day_pnl, "-");
  setPnlValue("tracked-paper-detail-cumulative-pnl", detail.cumulative_pnl, "-");
  text("tracked-paper-detail-signal", detail.latest_signal_summary || "-");
  text("tracked-paper-detail-intent", summarizeTrackedPaperEvent(detail.latest_order_intent, ["created_at", "intent_type", "reason_code", "order_status"]));
  text("tracked-paper-detail-fill", summarizeTrackedPaperEvent(detail.latest_fill, ["fill_timestamp", "intent_type", "fill_price", "order_status"]));
  text("tracked-paper-detail-exit", detail.latest_exit_reason || (detail.last_trade_summary && detail.last_trade_summary.exit_reason) || "-");
  text("tracked-paper-detail-risk", summarizeTrackedPaperRisk(detail.latest_stop_risk_context));
  text("tracked-paper-detail-status-reason", detail.status_reason || "-");
  text(
    "tracked-paper-detail-audit-summary",
    [
      `${(detail.recent_bars || []).length} bars`,
      `${(detail.recent_signals || []).length} signals`,
      `${(detail.recent_order_intents || []).length} intents`,
      `${(detail.recent_fills || []).length} fills`,
      `${(detail.recent_state_snapshots || []).length} state snapshots`,
      `${(detail.recent_faults || []).length} faults`,
      `${(detail.recent_reconciliation_events || []).length} reconciliation events`,
      `duplicate bars ${(detail.health_flags && detail.health_flags.duplicate_bar_suppression_count) ?? 0}`,
    ].join(" | "),
  );

  const startButton = document.getElementById("tracked-paper-start");
  const stopButton = document.getElementById("tracked-paper-stop");
  const haltButton = document.getElementById("tracked-paper-halt");
  const resumeButton = document.getElementById("tracked-paper-resume");
  const stopAfterCycleButton = document.getElementById("tracked-paper-stop-after-cycle");
  const flattenButton = document.getElementById("tracked-paper-flatten");
  if (startButton) {
    startButton.disabled = state.actionInFlight || detail.runtime_attached;
  }
  if (stopButton) {
    stopButton.disabled = state.actionInFlight || !detail.runtime_attached;
  }
  if (haltButton) {
    haltButton.disabled = state.actionInFlight || !detail.runtime_attached || detail.operator_halt;
  }
  if (resumeButton) {
    resumeButton.disabled = state.actionInFlight || !detail.runtime_attached || (!detail.operator_halt && detail.status !== "RECONCILING");
  }
  if (stopAfterCycleButton) {
    stopAfterCycleButton.disabled = state.actionInFlight || !detail.runtime_attached;
  }
  if (flattenButton) {
    flattenButton.disabled = state.actionInFlight || !detail.runtime_attached;
  }
}

function summarizeTrackedPaperEvent(payload, fields) {
  if (!payload || typeof payload !== "object") return "-";
  const parts = (Array.isArray(fields) ? fields : [])
    .map((field) => payload[field])
    .filter((value) => value !== undefined && value !== null && value !== "");
  return parts.length ? parts.join(" | ") : "-";
}

function summarizeTrackedPaperRisk(payload) {
  if (!payload || typeof payload !== "object") return "-";
  const preferredKeys = ["bias_state", "pullback_state", "entry_state", "timing_state", "vwap_price_quality_state", "exit_reason", "status", "entry_price"];
  const parts = preferredKeys
    .map((key) => payload[key] != null && payload[key] !== "" ? `${key}=${payload[key]}` : null)
    .filter(Boolean);
  return parts.length ? parts.join(" | ") : "-";
}

function normalizeTemporaryPaperStrategyForRoster(row) {
  const branch = String(row.display_name || row.branch || row.lane_id || "-");
  const blockedCount = Number(row.allow_block_override_summary?.blocked ?? (row.state === "DISABLED" ? 1 : 0));
  return {
    branch,
    source_family: row.observer_variant_id || row.source_family || row.lane_mode || "active_trend_participation_engine",
    lane_id: row.lane_id,
    instrument: row.instrument,
    session_restriction: row.session_restriction || "ALL",
    enabled: String(row.state || "").toUpperCase() === "ENABLED",
    state: row.state || "-",
    side: row.side || row.position_side || "-",
    signal_count: Number(row.recent_signal_count ?? row.signal_count ?? 0),
    blocked_count: blockedCount,
    intent_count: Number(row.intent_count ?? row.trade_count ?? 0),
    fill_count: Number(row.fill_count ?? row.trade_count ?? 0),
    open_position: Boolean(row.open_position),
    chain_state: row.lifecycle_state || (String(row.state || "").toUpperCase() === "ENABLED" ? "READY" : "IDLE"),
    realized_pnl: row.metrics_net_pnl_cash ?? row.realized_pnl ?? "N/A",
    unrealized_pnl: row.unrealized_pnl ?? "N/A",
    latest_activity_timestamp: row.last_update_timestamp || row.latest_activity_timestamp || row.fired_at || null,
    temporary_paper_strategy: true,
    paper_only: Boolean(row.paper_only),
    non_approved: Boolean(row.non_approved),
    experimental_status: row.experimental_status || "experimental_canary",
    quality_bucket_policy: row.quality_bucket_policy || "-",
  };
}

function normalizeTemporaryPaperStrategyDetail(row, artifacts = {}) {
  const branch = String(row.display_name || row.branch || row.lane_id || "-");
  const latestSignal = row.allow_block_override_summary?.label || row.latest_signal_label || "No signal yet";
  return {
    branch,
    side: row.side || row.position_side || "-",
    enabled: String(row.state || "").toUpperCase() === "ENABLED",
    open_position: Boolean(row.open_position),
    open_qty: 0,
    open_average_price: "-",
    persistence_state: "TEMPORARY PAPER STRATEGY",
    latest_signal_label: latestSignal,
    latest_blocked_timestamp: row.allow_block_override_summary?.blocked ? (row.last_update_timestamp || row.latest_activity_timestamp || null) : null,
    latest_blocked_reason: row.override_reason || row.allow_block_override_summary?.top_override_reason || null,
    latest_decision_timestamp: row.last_update_timestamp || row.latest_activity_timestamp || null,
    latest_intent_label: row.trade_count ? `${row.trade_count} paper trade(s) recorded` : "Observation only",
    latest_fill_label: row.trade_count ? `${row.trade_count} fill-equivalent paper trade(s)` : "No fill yet",
    unresolved_intent_count: 0,
    realized_pnl: row.metrics_net_pnl_cash ?? row.realized_pnl ?? null,
    unrealized_pnl: row.unrealized_pnl ?? null,
    reconciliation_state: "CLEAN",
    chain_state: row.lifecycle_state || "READY",
    chain_note: [
      "Experimental",
      row.paper_only ? "Paper Only" : null,
      row.non_approved ? "Non-Approved" : null,
      row.quality_bucket_policy ? `Quality ${row.quality_bucket_policy}` : null,
      row.note || null,
    ].filter(Boolean).join(" • "),
    artifacts: {
      decisions: row.artifacts?.signals || artifacts.decisions || null,
      blocks: artifacts.blocks || null,
      intents: row.artifacts?.events || artifacts.intents || null,
      fills: row.artifacts?.events || artifacts.fills || null,
      blotter: artifacts.blotter || null,
      position: artifacts.position || null,
      status: row.artifacts?.operator_status || artifacts.status || null,
      reconciliation: artifacts.reconciliation || null,
    },
    event_trail: [
      {
        timestamp: row.last_update_timestamp || row.latest_activity_timestamp || "-",
        title: "Temporary paper strategy update",
        detail: row.allow_block_override_summary?.label || row.note || "No recent event summary.",
      },
    ],
  };
}

function normalizeTemporaryPaperStrategyForActivity(row) {
  const branch = String(row.display_name || row.branch || row.lane_id || "-");
  const blocked = Number(row.allow_block_override_summary?.blocked || 0) > 0 || Boolean(row.kill_switch_active);
  const filled = Number(row.trade_count || row.fill_count || 0) > 0;
  return {
    branch,
    lane_id: row.lane_id,
    instrument: row.instrument,
    source_family: row.observer_variant_id || row.source_family || row.lane_mode || "active_trend_participation_engine",
    session_restriction: row.session_restriction || "ALL",
    verdict: blocked ? "BLOCKED" : (Number(row.recent_signal_count || row.signal_count || 0) > 0 ? "SIGNAL_ONLY" : "NO_ACTIVITY_YET"),
    latest_event_type: row.lifecycle_state || "NO_ACTIVITY",
    latest_event_timestamp: row.last_update_timestamp || row.latest_activity_timestamp || "-",
    has_signal_or_decision: Number(row.recent_signal_count || row.signal_count || 0) > 0,
    blocked,
    intent_open: false,
    filled,
    open_position: Boolean(row.open_position),
    latest_blocking_reason: row.override_reason || row.allow_block_override_summary?.top_override_reason || null,
    latest_fill_price: null,
    risk_state: row.kill_switch_active ? "KILL_SWITCH_ACTIVE" : (row.risk_state || "OK"),
    reconciliation_state: "CLEAN",
    temporary_paper_strategy: true,
    artifacts: {
      decisions: row.artifacts?.signals || null,
      blocks: row.artifacts?.events || null,
      fills: row.artifacts?.events || null,
      blotter: null,
      lane_risk: null,
      reconciliation: null,
    },
  };
}

function renderPaperNonApprovedLanes(payload) {
  const rows = Array.isArray(payload.rows) ? payload.rows : [];
  setBadge(
    "non-approved-lanes-badge",
    rows.length ? `${payload.temporary_paper_count ?? 0} TEMP PAPER / ${payload.total_count ?? rows.length} NON-APPROVED` : "NO NON-APPROVED LANES",
    rows.length ? "warning" : "muted",
  );
  text("non-approved-lanes-count", `${payload.total_count ?? rows.length}`);
  text("non-approved-lanes-canary-count", `${payload.canary_count ?? 0}`);
  text("non-approved-lanes-enabled-count", `${payload.enabled_count ?? 0}`);
  text("non-approved-lanes-disabled-count", `${payload.disabled_count ?? 0}`);
  text("non-approved-lanes-kill-switch", payload.kill_switch_active ? "ACTIVE" : "OFF");
  text("non-approved-lanes-recent-signals", `${payload.recent_signal_count ?? 0}`);
  text("non-approved-lanes-recent-events", `${payload.recent_event_count ?? 0}`);
  text("non-approved-lanes-complete-count", `${payload.completed_count ?? 0}`);
  text("non-approved-lanes-note", payload.note || payload.scope_label || "-");
  text("non-approved-lanes-operator-state", payload.operator_state_label || payload.operator_summary_line || "NO EXPERIMENTAL CANARY");
  text("non-approved-lanes-eligibility-source", payload.provenance?.eligibility || "-");
  text("non-approved-lanes-signal-source", payload.provenance?.signals || "-");
  text("non-approved-lanes-fill-source", payload.provenance?.fills || "-");
  text("non-approved-lanes-experimental-source", payload.provenance?.experimental_canaries || "-");
  setLink("paper-non-approved-lanes-link", payload.artifacts?.snapshot || null);
  setLink("paper-non-approved-lanes-status-link", payload.artifacts?.status || null);
  setLink("paper-non-approved-lanes-config-link", payload.artifacts?.config_in_force || null);
  setLink("paper-non-approved-lanes-decisions-link", payload.artifacts?.decisions || null);
  setLink("paper-non-approved-lanes-intents-link", payload.artifacts?.intents || null);
  setLink("paper-non-approved-lanes-fills-link", payload.artifacts?.fills || null);
  setLink("paper-non-approved-lanes-reconciliation-link", payload.artifacts?.reconciliation || null);
  setLink("paper-non-approved-lanes-experimental-link", payload.artifacts?.experimental_snapshot || null);
  setLink("paper-non-approved-lanes-experimental-summary-link", payload.artifacts?.experimental_operator_summary || null);

  const table = document.getElementById("non-approved-lanes-table");
  if (!table) return;
  table.className = "approved-models-table-compact";
  if (!rows.length) {
    table.innerHTML = "<tr><td>No paper-only non-approved lanes are active.</td></tr>";
    return;
  }
  table.innerHTML = `
    <thead>
      <tr>
        <th>instrument</th>
        <th>lane</th>
        <th>status</th>
        <th>policy</th>
        <th>side</th>
        <th>signals</th>
        <th>events</th>
        <th>allow/block/override</th>
        <th>lifecycle</th>
        <th>last update</th>
      </tr>
    </thead>
    <tbody>
      ${rows
        .map((row) => `
          <tr>
            <td class="approved-lane-instrument mono">${escapeHtml(row.instrument || "-")}</td>
            <td class="approved-lane-family">
              <div class="approved-lane-family-wrap">
                <span class="approved-lane-family-name mono">${escapeHtml(row.display_name || row.lane_id || "-")}</span>
                <span class="approved-lane-label subnote mono">${escapeHtml(row.lane_id || "-")}</span>
                <span class="approved-lane-label subnote mono">${escapeHtml(row.session_restriction || "-")}</span>
              </div>
            </td>
            <td>
              <div class="approved-lane-family-wrap">
                <span class="${badgeClass(row.kill_switch_active ? "danger" : (String(row.state || "").toUpperCase() === "ENABLED" ? "ok" : "warning"))}">${escapeHtml(row.state || "-")}</span>
                <span class="approved-lane-label subnote mono">${escapeHtml(row.kill_switch_active ? "KILL SWITCH ACTIVE" : "KILL SWITCH OFF")}</span>
              </div>
            </td>
            <td>
              <div class="approved-lane-family-wrap">
                <span class="${row.experimental_status === "experimental_canary" ? "badge badge-warning" : (row.is_canary ? "badge badge-warning" : "badge badge-muted")}">${escapeHtml(row.experimental_status === "experimental_canary" ? "EXPERIMENTAL" : (row.is_canary ? "CANARY" : "NON-APPROVED"))}</span>
                <span class="approved-lane-label subnote mono">${escapeHtml(row.temporary_paper_strategy ? "TEMPORARY PAPER STRATEGY" : "")}</span>
                <span class="approved-lane-label subnote mono">${escapeHtml(row.paper_only ? "PAPER ONLY" : (row.scope_label || "-"))}</span>
                <span class="approved-lane-label subnote mono">${escapeHtml(row.quality_bucket_policy || row.lane_mode || "-")}</span>
              </div>
            </td>
            <td>${escapeHtml(row.side || row.position_side || "FLAT")}</td>
            <td>${escapeHtml(String(row.recent_signal_count ?? row.signal_count ?? 0))}</td>
            <td>${escapeHtml(String(row.recent_event_count ?? row.event_count ?? 0))}</td>
            <td>${escapeHtml(row.allow_block_override_summary?.label || row.latest_signal_label || "-")}</td>
            <td><span class="${badgeClass(row.exit_completed ? "ok" : (row.entry_completed ? "warning" : "info"))}">${escapeHtml(row.lifecycle_state || "-")}</span></td>
            <td>${escapeHtml(row.last_update_timestamp || row.latest_activity_timestamp || row.fired_at || "-")}</td>
          </tr>
        `)
        .join("")}
    </tbody>
  `;
}

function approvedQuantProbationLevel(status) {
  switch ((status || "").toLowerCase()) {
    case "normal":
      return "ok";
    case "watch":
      return "warning";
    case "review":
    case "suspend":
      return "danger";
    default:
      return "muted";
  }
}

function formatSignedNumber(value) {
  if (value == null || value === "") return "-";
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return String(value);
  return `${numeric >= 0 ? "+" : ""}${numeric.toFixed(3)}`;
}

function renderPaperLaneActivity(payload, temporaryPayload = {}) {
  const summary = payload.summary || {};
  const approvedRows = Array.isArray(payload.rows) ? payload.rows : [];
  const temporaryRows = Array.isArray(temporaryPayload.rows) ? temporaryPayload.rows.map(normalizeTemporaryPaperStrategyForActivity) : [];
  const rows = [...approvedRows, ...temporaryRows];
  const blockedCount = rows.filter((row) => row.blocked).length;
  const filledCount = rows.filter((row) => row.filled).length;
  const openCount = rows.filter((row) => row.open_position).length;
  const anyActivity = rows.some((row) => row.verdict !== "NO_ACTIVITY_YET");
  setLink("paper-lane-activity-link", payload.artifacts?.snapshot || null);
  setStatusValue("paper-lane-activity-any", anyActivity ? "YES" : "NO", anyActivity ? "ok" : "muted");
  text("paper-lane-activity-idle", String(rows.filter((row) => row.verdict === "NO_ACTIVITY_YET").length));
  text("paper-lane-activity-blocked", String(blockedCount));
  text("paper-lane-activity-filled", String(filledCount));
  text("paper-lane-activity-open", String(openCount));
  text("paper-lane-activity-summary", temporaryRows.length ? `${payload.summary_line || "-"} Including ${temporaryRows.length} temporary paper strategy lane(s).` : (payload.summary_line || "-"));
  text("paper-lane-activity-provenance", payload.provenance || "-");

  const table = document.getElementById("paper-lane-activity-table");
  if (!table) return;
  table.className = "paper-lane-activity-table";
  if (!rows.length) {
    table.innerHTML = "<tr><td>No paper lane evidence is available yet.</td></tr>";
    return;
  }

  table.innerHTML = `
    <thead>
      <tr>
        <th>instrument</th>
        <th>family</th>
        <th>class</th>
        <th>session</th>
        <th>verdict</th>
        <th>signal/decision</th>
        <th>blocked</th>
        <th>intent</th>
        <th>fill</th>
        <th>open</th>
        <th>latest_event</th>
        <th>latest_ts</th>
        <th>block_reason</th>
        <th>fill_px</th>
        <th>risk</th>
        <th>evidence</th>
      </tr>
    </thead>
    <tbody>
      ${rows
        .map((row) => {
          const lane = parseLaneDisplay(row.branch);
          const links = [];
          if (row.has_signal_or_decision && row.artifacts?.decisions) {
            links.push(`<a class="link-button subtle-link" href="${escapeHtml(row.artifacts.decisions)}" target="_blank" rel="noopener noreferrer">Decisions</a>`);
          }
          if (row.blocked && row.artifacts?.blocks) {
            links.push(`<a class="link-button subtle-link" href="${escapeHtml(row.artifacts.blocks)}" target="_blank" rel="noopener noreferrer">Blocks</a>`);
          }
          if (row.intent_open && row.artifacts?.intents) {
            links.push(`<a class="link-button subtle-link" href="${escapeHtml(row.artifacts.intents)}" target="_blank" rel="noopener noreferrer">Intents</a>`);
          }
          if (row.filled && row.artifacts?.fills) {
            links.push(`<a class="link-button subtle-link" href="${escapeHtml(row.artifacts.fills)}" target="_blank" rel="noopener noreferrer">Fills</a>`);
          }
          if ((row.filled || row.open_position) && row.artifacts?.blotter) {
            links.push(`<a class="link-button subtle-link" href="${escapeHtml(row.artifacts.blotter)}" target="_blank" rel="noopener noreferrer">Blotter</a>`);
          }
          if (row.artifacts?.lane_risk) {
            links.push(`<a class="link-button subtle-link" href="${escapeHtml(row.artifacts.lane_risk)}" target="_blank" rel="noopener noreferrer">Risk</a>`);
          }
          if ((row.open_position || row.reconciliation_state === "DIRTY") && row.artifacts?.reconciliation) {
            links.push(`<a class="link-button subtle-link" href="${escapeHtml(row.artifacts.reconciliation)}" target="_blank" rel="noopener noreferrer">Recon</a>`);
          }
          return `
            <tr>
              <td class="approved-lane-instrument mono">${escapeHtml(row.instrument || lane.instrument || "-")}</td>
              <td class="approved-lane-family">
                <div class="approved-lane-family-wrap">
                  <span class="approved-lane-family-name mono">${escapeHtml(row.source_family || lane.family || "-")}</span>
                  <span class="approved-lane-label subnote mono">${escapeHtml(row.branch || "-")}</span>
                </div>
              </td>
              <td>${row.temporary_paper_strategy ? '<span class="badge badge-warning">TEMP PAPER</span>' : '<span class="badge badge-accent">ADMITTED</span>'}</td>
              <td>${sessionTagMarkup(row.session_restriction)}</td>
              <td><span class="${badgeClass(laneActivityVerdictLevel(row.verdict || "UNKNOWN_INSUFFICIENT_EVIDENCE"))}">${escapeHtml(row.verdict || "UNKNOWN_INSUFFICIENT_EVIDENCE")}</span></td>
              <td>${escapeHtml(row.has_signal_or_decision ? "YES" : "NO")}</td>
              <td>${escapeHtml(row.blocked ? "YES" : "NO")}</td>
              <td>${escapeHtml(row.intent_open ? "YES" : "NO")}</td>
              <td>${escapeHtml(row.filled ? "YES" : "NO")}</td>
              <td>${escapeHtml(row.open_position ? "YES" : "NO")}</td>
              <td>${escapeHtml(row.latest_event_type || "NO_ACTIVITY")}</td>
              <td>${escapeHtml(row.latest_event_timestamp || "-")}</td>
              <td>${escapeHtml(row.latest_blocking_reason || "-")}</td>
              <td>${escapeHtml(row.latest_fill_price || "-")}</td>
              <td><span class="${badgeClass(paperExceptionVerdictLevel(row.risk_state || "OK"))}">${escapeHtml(row.risk_state || "OK")}</span></td>
              <td><div class="lane-activity-links">${links.join("") || '<span class="subnote">No direct artifacts</span>'}</div></td>
            </tr>
          `;
        })
        .join("")}
    </tbody>
  `;
}

function renderPaperSoakSession(payload) {
  setBadge("paper-soak-end-verdict", payload.end_of_session_verdict || "UNKNOWN", paperSoakVerdictLevel(payload.end_of_session_verdict));
  text("paper-soak-start", payload.session_start || "No session evidence yet");
  text("paper-soak-duration", payload.runtime_duration || "Unavailable");
  text("paper-soak-seen", renderModelList(payload.approved_models_seen));
  text("paper-soak-signaled", renderModelList(payload.models_signaled));
  text("paper-soak-blocked", renderModelList(payload.models_blocked));
  text("paper-soak-intents", renderModelList(payload.models_intents));
  text("paper-soak-fills", renderModelList(payload.models_filled));
  text("paper-soak-open-now", renderModelList(payload.models_open_now));
  text("paper-soak-severe", payload.severe_exception_seen ? `YES (${payload.severe_exception_count ?? 0})` : "NO");
  setStatusValue(
    "paper-soak-current-verdict",
    payload.current_session_verdict || "UNKNOWN",
    paperExceptionVerdictLevel(payload.current_session_verdict),
  );
  setStatusValue(
    "paper-soak-summary-generated",
    payload.summary_generated ? "GENERATED" : "MISSING",
    payload.summary_generated ? "ok" : payload.summary_missing_warning ? "warning" : "muted",
  );
  text(
    "paper-soak-latest-evidence",
    payload.latest_evidence?.captured_at
      ? `${payload.latest_evidence.captured_at} • ${payload.latest_evidence.end_of_session_verdict || "-"}`
      : "No evidence bundle captured yet"
  );
  text("paper-soak-note", (payload.notes || []).join(" ") || "-");
  setLink("paper-soak-session-link", payload.artifacts?.session_snapshot || null);
  setLink("paper-soak-approved-link", payload.artifacts?.approved_models || null);
  setLink("paper-soak-exceptions-link", payload.artifacts?.exceptions || null);
  setLink("paper-soak-latest-json-link", payload.artifacts?.latest_json || null);
  setLink("paper-soak-latest-md-link", payload.artifacts?.latest_md || null);

  const captureButton = document.getElementById("capture-paper-soak-evidence");
  if (captureButton) {
    captureButton.disabled = state.actionInFlight || !payload.evidence_capture_available;
    captureButton.title = payload.evidence_capture_available
      ? "Write a current-session paper soak evidence bundle from persisted artifacts."
      : "Paper soak evidence capture is unavailable.";
  }
}

function historicalPlaybackResultLevel(status) {
  switch (status) {
    case "FIRED":
      return "ok";
    case "BLOCKED":
      return "warning";
    case "NO FIRE":
      return "muted";
    default:
      return "muted";
  }
}

function renderHistoricalPlayback(payload) {
  const run = payload.latest_run || {};
  const available = Boolean(payload.available && run.run_stamp);
  setBadge(
    "historical-playback-availability",
    available ? "LATEST RUN LOADED" : "NO RUN",
    available ? "info" : "muted"
  );
  text("historical-playback-run-timestamp", run.run_timestamp || "-");
  text("historical-playback-run-stamp", run.run_stamp || "-");
  text("historical-playback-symbols", (run.symbols || []).join(", ") || "-");
  text("historical-playback-bars", run.bars_processed != null ? String(run.bars_processed) : "-");
  text("historical-playback-signals", run.signals_seen != null ? String(run.signals_seen) : "-");
  text("historical-playback-intents", run.intents_created != null ? String(run.intents_created) : "-");
  text("historical-playback-fills", run.fills_created != null ? String(run.fills_created) : "-");
  text(
    "historical-playback-result-mix",
    available
      ? `FIRED ${run.fired_count || 0} • NO FIRE ${run.no_fire_count || 0} • BLOCKED ${run.blocked_count || 0}`
      : "-"
  );
  text(
    "historical-playback-note",
    payload.note || "Historical playback results are shown separately from live/paper operator state."
  );
  const rows = Array.isArray(run.rows) ? run.rows : [];
  const dominantStatus = rows.find((row) => row.result_status === "FIRED")?.result_status
    || rows.find((row) => row.result_status === "BLOCKED")?.result_status
    || rows[0]?.result_status
    || "NO RUN";
  if (available) {
    setBadge(
      "historical-playback-availability",
      dominantStatus,
      historicalPlaybackResultLevel(dominantStatus)
    );
  }
  setLink("historical-playback-snapshot-link", payload.artifacts?.snapshot || null);
  setLink("historical-playback-manifest-link", run.artifacts?.manifest || null);
  setLink("historical-playback-summary-link", run.artifacts?.summary || null);
  setLink("historical-playback-trigger-json-link", run.artifacts?.trigger_report_json || null);
  setLink("historical-playback-trigger-md-link", run.artifacts?.trigger_report_markdown || null);
}

function paperEntryEligibilityLevel(verdict) {
  switch (verdict) {
    case "ELIGIBLE TO FIRE":
      return "ok";
    case "NOT ELIGIBLE: ENTRIES HALTED BY OPERATOR":
    case "NOT ELIGIBLE: RUNTIME STOPPED":
      return "warning";
    case "NOT ELIGIBLE: OPEN-RISK / REVIEW REQUIRED":
    case "NOT ELIGIBLE: STARTUP / REVIEW GATING":
    case "NOT ELIGIBLE: NO APPROVED MODELS ENABLED":
      return "warning";
    case "NOT ELIGIBLE: FAULTED":
    case "NOT ELIGIBLE: RECONCILIATION DIRTY":
      return "danger";
    default:
      return "muted";
  }
}

function paperEntryActionLevel(action) {
  if (!action) return "muted";
  if (action === "No action needed; already eligible") return "ok";
  if (action === "Manual inspection required") return "danger";
  return "warning";
}

function paperActivityProofLevel(verdict) {
  switch (verdict) {
    case "PAPER DESK SHOWING LIVE MODEL ACTIVITY":
      return "ok";
    case "PAPER DESK RUNNING BUT NO APPROVED-MODEL ACTIVITY YET":
      return "warning";
    case "PAPER DESK NOT ACTUALLY RUNNING / NOT POLLING":
      return "danger";
    default:
      return "muted";
  }
}

function renderApprovedModelDetail(detail, artifacts) {
  if (!detail) {
    setBadge("approved-model-detail-state", "NO LANE", "muted");
    text("approved-model-detail-branch", "-");
    text("approved-model-detail-side", "-");
    text("approved-model-detail-enabled", "-");
    text("approved-model-detail-open", "-");
    text("approved-model-detail-open-meta", "-");
    text("approved-model-detail-persistence", "-");
    text("approved-model-detail-signal", "-");
    text("approved-model-detail-block", "-");
    text("approved-model-detail-decision", "-");
    text("approved-model-detail-intent", "-");
    text("approved-model-detail-fill", "-");
    text("approved-model-detail-unresolved", "-");
    text("approved-model-detail-realized", "-");
    text("approved-model-detail-unrealized", "-");
    text("approved-model-detail-reconciliation", "-");
    text("approved-model-detail-note", "No paper strategy detail is currently available.");
    [
      "approved-model-detail-decisions-link",
      "approved-model-detail-blocks-link",
      "approved-model-detail-intents-link",
      "approved-model-detail-fills-link",
      "approved-model-detail-blotter-link",
      "approved-model-detail-position-link",
      "approved-model-detail-status-link",
      "approved-model-detail-reconciliation-link",
    ].forEach((id) => setLink(id, null));
    renderApprovedModelEventTrail([]);
    return;
  }

  setBadge("approved-model-detail-state", detail.chain_state || "UNKNOWN", approvedModelChainLevel(detail.chain_state));
  text("approved-model-detail-branch", detail.branch || "-");
  text("approved-model-detail-side", detail.side || "-");
  text("approved-model-detail-enabled", detail.enabled ? "ENABLED" : "DISABLED");
  setStatusValue("approved-model-detail-open", detail.open_position ? "OPEN" : "FLAT", detail.open_position ? "warning" : "ok");
  text(
    "approved-model-detail-open-meta",
    detail.open_position
      ? `${detail.open_qty ?? 0} @ ${detail.open_average_price || "-"}`
      : "No open exposure"
  );
  text("approved-model-detail-persistence", detail.persistence_state || "-");
  text("approved-model-detail-signal", detail.latest_signal_label || "No signal yet");
  text(
    "approved-model-detail-block",
    detail.latest_blocked_timestamp
      ? `${detail.latest_blocked_timestamp} • ${detail.latest_blocked_reason || "-"}`
      : "No block seen"
  );
  text("approved-model-detail-decision", detail.latest_decision_timestamp || "No decision yet");
  text("approved-model-detail-intent", detail.latest_intent_label || "No intent yet");
  text("approved-model-detail-fill", detail.latest_fill_label || "No fill yet");
  text("approved-model-detail-unresolved", String(detail.unresolved_intent_count ?? 0));
  setPnlValue("approved-model-detail-realized", detail.realized_pnl, "Unavailable");
  setPnlValue("approved-model-detail-unrealized", detail.unrealized_pnl, "Unavailable");
  setStatusValue(
    "approved-model-detail-reconciliation",
    detail.reconciliation_state || "UNKNOWN",
    detail.reconciliation_state === "CLEAN" ? "ok" : detail.reconciliation_state ? "warning" : "muted",
  );
  text("approved-model-detail-note", detail.chain_note || "-");

  setLink("approved-model-detail-decisions-link", detail.artifacts?.decisions || artifacts.decisions || null);
  setLink("approved-model-detail-blocks-link", detail.artifacts?.blocks || artifacts.blocks || null);
  setLink("approved-model-detail-intents-link", detail.artifacts?.intents || artifacts.intents || null);
  setLink("approved-model-detail-fills-link", detail.artifacts?.fills || artifacts.fills || null);
  setLink("approved-model-detail-blotter-link", detail.artifacts?.blotter || artifacts.blotter || null);
  setLink("approved-model-detail-position-link", detail.artifacts?.position || artifacts.position || null);
  setLink("approved-model-detail-status-link", detail.artifacts?.status || artifacts.status || null);
  setLink("approved-model-detail-reconciliation-link", detail.artifacts?.reconciliation || artifacts.reconciliation || null);
  renderApprovedModelEventTrail(detail.event_trail || []);
}

function renderApprovedModelEventTrail(events) {
  const target = document.getElementById("approved-model-event-trail");
  const badge = document.getElementById("approved-model-trail-state");
  target.innerHTML = "";
  if (!events.length) {
    if (badge) {
      setBadge("approved-model-trail-state", "NO EVENTS", "muted");
    }
    target.innerHTML = "<li>No paper-session events are currently attributable to this paper strategy.</li>";
    return;
  }
  if (badge) {
    setBadge("approved-model-trail-state", `${events.length} EVENTS`, "info");
  }
  events.forEach((event) => {
    const item = document.createElement("li");
    item.innerHTML = `
      <div class="model-event-top">
        <div class="timeline-title-row">
          <span class="${timelineBadgeClass(event.category)}">${escapeHtml(String(event.category || "event").toUpperCase())}</span>
          <span class="timeline-title">${escapeHtml(event.title || "-")}</span>
        </div>
        <span class="timeline-time">${escapeHtml(event.timestamp || "-")}</span>
      </div>
      <div class="model-event-details">${escapeHtml(event.details || "-")}</div>
      <div class="timeline-provenance">${escapeHtml(event.provenance || "-")}</div>
      ${event.artifact_href ? `<div class="model-event-links"><a class="link-button subtle-link" href="${escapeHtml(event.artifact_href)}" target="_blank" rel="noopener noreferrer">${escapeHtml(event.artifact_label || "Artifact")}</a></div>` : ""}
    `;
    target.appendChild(item);
  });
}

function renderPaperRiskBanner(payload) {
  const banner = document.getElementById("paper-risk-banner");
  const ackButton = document.getElementById("acknowledge-paper-risk");
  if (!payload.active) {
    banner.classList.add("hidden");
    ackButton.disabled = true;
    return;
  }
  banner.classList.remove("hidden");
  setBadge("paper-risk-ack-state", payload.acknowledged ? "ACKNOWLEDGED" : "UNACKNOWLEDGED", payload.acknowledged ? "warning" : "danger");
  text("paper-risk-title", payload.acknowledged ? "Paper Risk Acknowledged" : "Unresolved Paper Risk");
  text(
    "paper-risk-body",
    [...(payload.reasons || []), "", `Guidance: ${payload.guidance || "-"}`, payload.acknowledged_at ? `Acknowledged at: ${payload.acknowledged_at}` : "Acknowledgment required for operator sign-off."].join("\n"),
  );
  ackButton.disabled = state.actionInFlight || payload.acknowledged;
}

function renderPaperCloseout(payload) {
  setBadge(
    "paper-closeout-state",
    payload.reviewed ? "REVIEWED" : payload.sign_off_available ? "READY TO REVIEW" : "OPEN",
    payload.reviewed ? "ok" : payload.sign_off_available ? "warning" : "danger",
  );
  setStatusValue("close-runtime-state", payload.runtime_running ? "RUNNING" : "STOPPED", payload.runtime_running ? "warning" : "ok");
  setStatusValue(
    "close-position-state",
    payload.position_flat ? "FLAT" : payload.position_side || "NOT FLAT",
    payload.position_flat ? "ok" : "danger",
  );
  setStatusValue("close-recon-state", payload.reconciliation_clean ? "CLEAN" : "DIRTY", payload.reconciliation_clean ? "ok" : "danger");
  text("close-unresolved-count", `${payload.unresolved_open_intents ?? 0}`);
  setStatusValue("close-risk-ack", payload.risk_acknowledged ? "ACKED" : payload.risk_active ? "REQUIRED" : "NONE", payload.risk_acknowledged ? "ok" : payload.risk_active ? "warning" : "muted");
  setStatusValue("close-summary-state", payload.summary_generated ? "GENERATED" : "MISSING", payload.summary_generated ? "ok" : "warning");
  setStatusValue("close-blotter-state", payload.blotter_generated ? "AVAILABLE" : "MISSING", payload.blotter_generated ? "ok" : "warning");
  setStatusValue("close-fault-state", payload.fault_state || "CLEAR", payload.fault_state === "FAULTED" ? "danger" : "ok");
  setStatusValue("close-freshness-state", payload.freshness || "-", payload.freshness === "LIVE" ? "ok" : payload.freshness === "STALE" ? "warning" : "muted");
  text("close-guidance", payload.guidance || "-");
  text(
    "close-signoff-meta",
    payload.reviewed
      ? `Reviewed ${payload.reviewed_at || "-"}`
      : `Session ${payload.session_date || "-"}`
  );

  const warningBox = document.getElementById("closeout-warning");
  if (payload.warning_reasons?.length) {
    warningBox.classList.remove("hidden");
    warningBox.textContent = payload.warning_reasons.join("\n");
  } else {
    warningBox.classList.add("hidden");
    warningBox.textContent = "";
  }

  const checklist = document.getElementById("close-checklist");
  checklist.innerHTML = "";
  if (!(payload.checklist || []).length) {
    checklist.innerHTML = "<li>No close checklist data yet.</li>";
  } else {
    payload.checklist.forEach((item) => {
      const row = document.createElement("li");
      row.innerHTML = `
        <div><strong>${escapeHtml(item.label || "-")}</strong> <span class="status-${statusLevelForChecklist(item.status)}">${escapeHtml(String(item.status || "-").toUpperCase())}</span></div>
        <div>${escapeHtml(item.guidance || "-")}</div>
      `;
      checklist.appendChild(row);
    });
  }

  const signoffButton = document.getElementById("paper-signoff");
  if (signoffButton) {
    signoffButton.disabled = state.actionInFlight || !payload.sign_off_available;
    signoffButton.title = payload.sign_off_available
      ? "Record the paper session close review."
      : "Sign-off requires a stopped paper runtime, generated summary, and any remaining risk acknowledged.";
  }
}

function paperSessionCloseVerdictLevel(verdict) {
  switch (verdict) {
    case "CLEAN_IDLE":
    case "CLEAN_WITH_ACTIVITY":
      return "accent";
    case "OPEN_RISK_REMAINS":
    case "HALTED_WITH_OPEN_RISK":
      return "warning";
    case "DIRTY_CLOSE":
    case "FAULTED_CLOSE":
      return "danger";
    default:
      return "muted";
  }
}

function paperSessionLaneVerdictLevel(verdict) {
  switch (verdict) {
    case "FILLED_AND_FLAT":
      return "accent";
    case "FILLED_WITH_OPEN_RISK":
    case "HALTED_BY_RISK":
    case "DIRTY_RECONCILIATION":
      return "danger";
    case "SIGNAL_NO_FILL":
      return "info";
    case "BLOCKED_ONLY":
      return "warning";
    case "IDLE":
      return "muted";
    default:
      return "muted";
  }
}

function reviewConfidenceLevel(confidence) {
  switch (confidence) {
    case "COMPLETE":
    case "HIGH":
    case "REVIEW_TRUST_HIGH":
      return "accent";
    case "PARTIAL":
    case "MEDIUM":
    case "REVIEW_TRUST_MEDIUM":
      return "info";
    case "LOW":
    case "REVIEW_TRUST_LOW":
      return "warning";
    case "BROKEN":
    case "NONE":
    case "REVIEW_TRUST_NONE":
      return "danger";
    default:
      return "muted";
  }
}

function renderPaperSessionCloseReview(payload) {
  setBadge("paper-session-close-review-badge", payload.desk_close_verdict || "UNKNOWN_INSUFFICIENT_EVIDENCE", paperSessionCloseVerdictLevel(payload.desk_close_verdict));
  setLink("paper-session-close-review-json-link", payload.artifacts?.snapshot_json || null);
  setLink("paper-session-close-review-md-link", payload.artifacts?.snapshot_md || null);
  text("paper-session-close-admitted", String(payload.admitted_lanes_count ?? 0));
  text("paper-session-close-active", String(payload.active_lanes_count ?? 0));
  text("paper-session-close-blocked", String(payload.blocked_lanes_count ?? 0));
  text("paper-session-close-filled", String(payload.filled_lanes_count ?? 0));
  text("paper-session-close-open", String(payload.open_lanes_count ?? 0));
  text("paper-session-close-realized", `${payload.total_attributable_realized_pnl ?? "-"} • ${payload.realized_attribution_coverage || "-"}`);
  setStatusValue("paper-session-close-confidence", payload.desk_attribution_summary?.desk_review_confidence || "NONE", reviewConfidenceLevel(payload.desk_attribution_summary?.desk_review_confidence));
  setStatusValue("paper-session-close-completeness", payload.desk_attribution_summary?.desk_pnl_completeness || "-", payload.desk_attribution_summary?.desk_pnl_completeness === "COMPLETE" ? "ok" : "warning");
  text("paper-session-close-review-required", (payload.review_required_lanes || []).join(", ") || "None");
  text("paper-session-close-reliable", (payload.desk_attribution_summary?.reliable_pnl_judgment_lanes || []).join(", ") || "None");
  text("paper-session-close-manual", (payload.desk_attribution_summary?.manual_pnl_inspection_lanes || []).join(", ") || "None");
  text("paper-session-close-chain-complete", (payload.desk_attribution_summary?.complete_evidence_chain_lanes || []).join(", ") || "None");
  text("paper-session-close-chain-partial", (payload.desk_attribution_summary?.partial_evidence_chain_lanes || []).join(", ") || "None");
  text("paper-session-close-chain-broken", (payload.desk_attribution_summary?.broken_evidence_chain_lanes || []).join(", ") || "None");
  text(
    "paper-session-close-gap-top",
    (payload.desk_attribution_summary?.top_attribution_gap_reasons || [])
      .map((item) => `${item.reason} (${item.count})`)
      .join(", ") || "None",
  );
  setStatusValue(
    "paper-session-close-history-verdict",
    payload.desk_attribution_summary?.historical_trust_verdict || "CLOSE_HISTORY_MIXED",
    reviewConfidenceLevel(
      payload.desk_attribution_summary?.historical_trust_verdict === "CLOSE_HISTORY_CLEAN"
        ? "HIGH"
        : payload.desk_attribution_summary?.historical_trust_verdict === "CLOSE_HISTORY_REVIEW_REQUIRED"
          ? "NONE"
          : "MEDIUM",
    ),
  );
  setStatusValue(
    "paper-session-close-history-confidence",
    payload.desk_attribution_summary?.desk_history_confidence || "LOW",
    reviewConfidenceLevel(payload.desk_attribution_summary?.desk_history_confidence || "LOW"),
  );
  text(
    "paper-session-close-history-repeats",
    [
      ...(payload.desk_attribution_summary?.repeated_partial_chain_lanes || []),
      ...(payload.desk_attribution_summary?.repeated_broken_chain_lanes || []),
      ...(payload.desk_attribution_summary?.repeated_dirty_close_lanes || []),
      ...(payload.desk_attribution_summary?.repeated_open_risk_close_lanes || []),
    ].join(", ") || "None",
  );
  text("paper-session-close-history-note", payload.desk_attribution_summary?.history_sufficiency_note || "No archived close-review history summary available.");
  const sourcePaths = payload.source_paths || {};
  text(
    "paper-session-close-source-paths",
    Object.entries(sourcePaths)
      .filter(([, value]) => Boolean(value))
      .map(([key]) => key)
      .join(" | ") || "-",
  );
  text("paper-session-close-notes", (payload.notes || []).join(" ") || "-");

  const table = document.getElementById("paper-session-close-table");
  if (!table) return;
  table.className = "paper-session-close-table";
  const rows = Array.isArray(payload.rows) ? payload.rows : [];
  if (!rows.length) {
    table.innerHTML = "<tr><td>No multi-lane session-close review is available yet.</td></tr>";
    return;
  }
  table.innerHTML = `
    <thead>
      <tr>
        <th>instrument</th>
        <th>family</th>
        <th>session</th>
        <th>verdict</th>
        <th>signals</th>
        <th>blocked</th>
        <th>intents</th>
        <th>fills</th>
        <th>realized</th>
        <th>realized_attr</th>
        <th>open</th>
        <th>open_attr</th>
        <th>chain</th>
        <th>trust</th>
        <th>hist</th>
        <th>repeat_conf</th>
        <th>prior</th>
        <th>repeat</th>
        <th>last_issue</th>
        <th>latest_ts</th>
        <th>halt_reason</th>
        <th>gap_reason</th>
        <th>open_first</th>
        <th>history</th>
        <th>evidence</th>
      </tr>
    </thead>
    <tbody>
      ${rows
        .map((row) => `
          <tr>
            <td class="approved-lane-instrument mono">${escapeHtml(row.instrument || "-")}</td>
            <td class="approved-lane-family">
              <div class="approved-lane-family-wrap">
                <span class="approved-lane-family-name mono">${escapeHtml(row.source_family || "-")}</span>
                <span class="approved-lane-label subnote mono">${escapeHtml(row.branch || "-")}</span>
              </div>
            </td>
            <td>${sessionTagMarkup(row.session_restriction)}</td>
            <td><span class="${badgeClass(paperSessionLaneVerdictLevel(row.session_verdict || "UNKNOWN_INSUFFICIENT_EVIDENCE"))}">${escapeHtml(row.session_verdict || "UNKNOWN_INSUFFICIENT_EVIDENCE")}</span></td>
            <td>${escapeHtml(row.signal_count ?? 0)}</td>
            <td>${escapeHtml(row.blocked_count ?? 0)}</td>
            <td>${escapeHtml(row.intent_count ?? 0)}</td>
            <td>${escapeHtml(row.fill_count ?? 0)}</td>
            <td>${escapeHtml(row.realized_pnl_attribution_status === "UNATTRIBUTABLE" ? "Unavailable" : (row.attributable_realized_pnl ?? "-"))}</td>
            <td><span class="${badgeClass(reviewConfidenceLevel(row.realized_pnl_attribution_status || "UNATTRIBUTABLE"))}">${escapeHtml(row.realized_pnl_attribution_status || "UNATTRIBUTABLE")}</span></td>
            <td>${escapeHtml(row.open_position ? "YES" : "NO")}</td>
            <td><span class="${badgeClass(reviewConfidenceLevel(row.unrealized_pnl_attribution_status || "UNATTRIBUTABLE"))}">${escapeHtml(row.unrealized_pnl_attribution_status || "UNATTRIBUTABLE")}</span></td>
            <td><span class="${badgeClass(reviewConfidenceLevel(row.evidence_chain_status || "BROKEN"))}">${escapeHtml(row.evidence_chain_status || "BROKEN")}</span></td>
            <td><span class="${badgeClass(reviewConfidenceLevel(row.review_confidence || "REVIEW_TRUST_NONE"))}">${escapeHtml(row.review_confidence || "REVIEW_TRUST_NONE")}</span></td>
            <td><span class="${badgeClass(reviewConfidenceLevel(
              row.history_sufficiency_status === "HISTORY_SUFFICIENT"
                ? "HIGH"
                : "LOW",
            ))}">${escapeHtml(row.history_sufficiency_status || "HISTORY_NONE")}</span></td>
            <td><span class="${badgeClass(reviewConfidenceLevel(row.repeat_review_confidence || "LOW"))}">${escapeHtml(row.repeat_review_confidence || "LOW")}</span></td>
            <td>${escapeHtml(row.prior_close_reviews_found ?? 0)}</td>
            <td><span class="${badgeClass(reviewConfidenceLevel(
              row.repeat_review_verdict === "NO_REPEAT_ISSUE_SEEN"
                ? (row.clean_history_judgment_allowed ? "HIGH" : "LOW")
                : row.repeat_review_verdict === "MANUAL_REVIEW_PATTERN"
                  ? "NONE"
                  : "LOW",
            ))}">${escapeHtml(row.repeat_review_verdict || "NO_REPEAT_ISSUE_SEEN")}</span></td>
            <td>${escapeHtml(row.last_manual_review_required_ts || row.last_broken_close_ts || row.last_partial_close_ts || "-")}</td>
            <td>${escapeHtml(row.latest_event_timestamp || "-")}</td>
            <td>${escapeHtml(row.latest_halt_reason || "-")}</td>
            <td>${escapeHtml((row.attribution_gap_reason || [])[0] || "-")}</td>
            <td>
              <a class="link-button subtle-link" href="${escapeHtml(row.open_first_recommendation?.href || "#")}" target="_blank" rel="noopener noreferrer">
                ${escapeHtml(row.open_first_recommendation?.label || "-")}
              </a>
            </td>
            <td>
              <div class="lane-activity-links">
                <a class="link-button subtle-link" href="${escapeHtml(row.history_artifacts?.inventory_json || "#")}" target="_blank" rel="noopener noreferrer">History</a>
              </div>
            </td>
            <td>
              <div class="lane-activity-links">
                <a class="link-button subtle-link" href="${escapeHtml(row.artifacts?.decisions || "#")}" target="_blank" rel="noopener noreferrer">Decisions</a>
                <a class="link-button subtle-link" href="${escapeHtml(row.artifacts?.blocks || "#")}" target="_blank" rel="noopener noreferrer">Blocks</a>
                <a class="link-button subtle-link" href="${escapeHtml(row.artifacts?.intents || "#")}" target="_blank" rel="noopener noreferrer">Intents</a>
                <a class="link-button subtle-link" href="${escapeHtml(row.artifacts?.fills || "#")}" target="_blank" rel="noopener noreferrer">Fills</a>
                <a class="link-button subtle-link" href="${escapeHtml(row.artifacts?.blotter || "#")}" target="_blank" rel="noopener noreferrer">Blotter</a>
                <a class="link-button subtle-link" href="${escapeHtml(row.artifacts?.lane_risk || "#")}" target="_blank" rel="noopener noreferrer">Risk</a>
                <a class="link-button subtle-link" href="${escapeHtml(row.artifacts?.reconciliation || "#")}" target="_blank" rel="noopener noreferrer">Recon</a>
                <a class="link-button subtle-link" href="${escapeHtml(row.artifacts?.position || "#")}" target="_blank" rel="noopener noreferrer">Position</a>
              </div>
            </td>
          </tr>
        `)
        .join("")}
    </tbody>
  `;
}

function renderCarryForward(payload) {
  const banner = document.getElementById("carry-forward-banner");
  const ackButton = document.getElementById("acknowledge-inherited-risk");
  const resolveButton = document.getElementById("resolve-inherited-risk");

  setBadge(
    "prior-session-state",
    payload.active ? (payload.resolution_eligible ? "READY TO CLEAR" : "GUARDED") : payload.resolved ? "CLEARED" : "NONE",
    payload.active ? (payload.resolution_eligible ? "warning" : "danger") : payload.resolved ? "ok" : "muted",
  );

  if (!payload.active) {
    banner.classList.add("hidden");
    text(
      "prior-session-meta",
      payload.resolved
        ? `Prior session ${payload.session_date || "-"} was explicitly cleared at ${payload.resolved_at || "-"}.`
        : "No inherited prior-session risk is currently active.",
    );
    setLink("prior-summary-json", payload.summary_links?.json || null);
    setLink("prior-summary-md", payload.summary_links?.md || null);
    setLink("prior-summary-blotter", payload.summary_links?.blotter || null);
    if (ackButton) ackButton.disabled = true;
    if (resolveButton) resolveButton.disabled = true;
    return;
  }

  banner.classList.remove("hidden");
  setBadge(
    "carry-forward-state",
    payload.resolution_eligible ? "READY TO CLEAR" : payload.acknowledged ? "ACKNOWLEDGED" : "ACTIVE",
    payload.resolution_eligible ? "warning" : payload.acknowledged ? "warning" : "danger",
  );
  text(
    "carry-forward-title",
    payload.resolution_eligible ? "Prior Session Remediated But Not Cleared" : "Previous Session Not Clean",
  );
  text(
    "carry-forward-body",
    [
      `Prior session date: ${payload.session_date || "-"}`,
      ...(payload.reasons || []),
      "",
      `Guidance: ${payload.guidance || "-"}`,
      payload.acknowledged_at ? `Acknowledged at: ${payload.acknowledged_at}` : "Acknowledgment required before operator clean sign-off.",
      payload.reviewed_at ? `Prior reviewed at: ${payload.reviewed_at}` : "Prior session review missing.",
    ].join("\n"),
  );
  text(
    "prior-session-meta",
    [
      `Prior session date: ${payload.session_date || "-"}`,
      `Reviewed: ${payload.reviewed ? "YES" : "NO"}`,
      `Not flat at close: ${payload.not_flat_at_close ? "YES" : "NO"}`,
      `Reconciliation dirty: ${payload.reconciliation_dirty ? "YES" : "NO"}`,
      `Unresolved intents/orders: ${payload.unresolved_open_intents ?? 0}`,
      `Summary generated: ${payload.summary_generated ? "YES" : "NO"}`,
      `Blotter generated: ${payload.blotter_generated ? "YES" : "NO"}`,
      `Risk acknowledged: ${payload.acknowledged ? "YES" : "NO"}`,
      payload.resolved_at ? `Resolved at: ${payload.resolved_at}` : "Resolved at: -",
    ].join("\n"),
  );
  setLink("prior-summary-json", payload.summary_links?.json || null);
  setLink("prior-summary-md", payload.summary_links?.md || null);
  setLink("prior-summary-blotter", payload.summary_links?.blotter || null);
  if (ackButton) {
    ackButton.disabled = state.actionInFlight || payload.acknowledged;
  }
  if (resolveButton) {
    resolveButton.disabled = state.actionInFlight || !payload.resolution_eligible;
    resolveButton.title = payload.resolution_eligible
      ? "Explicitly clear inherited prior-session risk."
      : "Resolve is only available after the previous session is actually remediated.";
  }
}

function renderPreSessionReview(global, payload, carryForward) {
  setBadge(
    "pre-session-state",
    payload.readiness_label || "UNKNOWN",
    payload.ready_for_run ? "ok" : "warning",
  );
  setStatusValue("pre-desk-clean", global.desk_clean ? "CLEAN" : "GUARDED", global.desk_clean ? "ok" : "warning");
  setStatusValue("pre-runtime-health", global.runtime_health_label || "-", levelForHealth(global.runtime_health_label));
  setStatusValue("pre-run-readiness", payload.ready_for_run ? "READY FOR RUN" : "REVIEW PENDING", payload.ready_for_run ? "ok" : "warning");
  setStatusValue("pre-inherited-risk", carryForward.active ? "ACTIVE" : "CLEAR", carryForward.active ? "warning" : "ok");
  setStatusValue("pre-review-required", payload.required ? "YES" : "NO", payload.required ? "warning" : "ok");
  setStatusValue("pre-review-completed", payload.completed ? "YES" : "NO", payload.completed ? "ok" : payload.required ? "warning" : "muted");
  text("pre-session-guidance", payload.guidance || "-");

  const reviewButton = document.getElementById("complete-pre-session-review");
  if (reviewButton) {
    reviewButton.disabled = state.actionInFlight || !payload.required || payload.completed;
    reviewButton.title = payload.required
      ? payload.completed
        ? "Guarded startup review already recorded for this inherited-risk context."
        : "Record that the guarded startup condition has been reviewed."
      : "No guarded startup review is currently required.";
  }

  const startPaperButton = document.querySelector('button[data-action="start-paper"]');
  if (startPaperButton && !state.actionInFlight) {
    startPaperButton.dataset.interlocked = payload.ready_for_run ? "false" : "true";
    startPaperButton.disabled = !payload.ready_for_run;
    startPaperButton.title = payload.ready_for_run
      ? "Start paper soak."
      : "Paper start is interlocked until the guarded startup review is completed.";
  }
}

function renderPaperRunStart(payload) {
  const current = payload.current;
  if (!current) {
    setBadge("run-start-state", "NO RUN START", "muted");
    text("run-start-meta", "No paper run start has been recorded yet.");
  } else {
    setBadge("run-start-state", current.desk_state_at_start === "GUARDED" ? "STARTED GUARDED" : "STARTED CLEAN", current.desk_state_at_start === "GUARDED" ? "warning" : "ok");
    text(
      "run-start-meta",
      [
        `Started: ${current.timestamp || "-"}`,
        `Session: ${current.session_date || "-"}`,
        `Run start id: ${current.run_start_id || "-"}`,
        `Desk state: ${current.desk_state_at_start || "-"}`,
        `Inherited risk: ${current.inherited_risk_active ? "YES" : "NO"}`,
        `Pre-session review required: ${current.pre_session_review_required ? "YES" : "NO"}`,
        `Pre-session review completed: ${current.pre_session_review_completed ? "YES" : "NO"}`,
        `Started after guarded review: ${current.started_after_guarded_review ? "YES" : "NO"}`,
        `Runtime health at start: ${current.runtime_health_at_start || "-"}`,
        `Reconciliation at start: ${current.reconciliation_state_at_start || "-"}`,
        `Unresolved orders at start: ${current.unresolved_orders_at_start ?? 0}`,
      ].join("\n"),
    );
  }

  setLink("run-start-current-link", payload.links?.current || null);
  setLink("run-start-history-link", payload.links?.starts || null);
  setLink("run-start-blocks-link", payload.links?.blocked || null);

  const blockedTarget = document.getElementById("blocked-starts");
  blockedTarget.innerHTML = "";
  const blockedRows = payload.blocked_history || [];
  if (!blockedRows.length) {
    blockedTarget.innerHTML = "<li>No blocked paper-start attempts recorded.</li>";
    return;
  }
  blockedRows
    .slice()
    .reverse()
    .forEach((row) => {
      const item = document.createElement("li");
      item.innerHTML = `
        <div><strong>${escapeHtml(row.timestamp || "-")}</strong> <span class="status-warning">${escapeHtml(row.desk_state_at_attempt || "UNKNOWN")}</span></div>
        <div>${escapeHtml(row.blocked_reason || "-")}</div>
      `;
      blockedTarget.appendChild(item);
    });
}

function controlButtonId(label) {
  return {
    "Buy": "manual-buy",
    "Sell": "manual-sell",
    "Flatten And Halt": "manual-flatten",
    "Halt Entries": "manual-halt-entries",
    "Resume Entries": "manual-resume-entries",
    "Acknowledge/Clear Fault": "manual-clear-fault",
    "Stop After Current Cycle": "manual-stop-after-cycle",
    "Kill Switch": "manual-kill-switch",
  }[label];
}

function setRefreshSelect() {
  const select = document.getElementById("refresh-interval");
  if (!select) return;
  if (String(state.refreshIntervalSeconds) !== select.value) {
    select.value = String(state.refreshIntervalSeconds);
  }
}

function renderPaperContinuity(payload) {
  setLink("continuity-link", payload.links?.continuity || null);
  setLink("continuity-prior-json", payload.links?.prior_summary_json || null);
  setLink("continuity-prior-blotter", payload.links?.prior_summary_blotter || null);
  setLink("continuity-carry-link", payload.links?.carry_forward || null);
  setLink("continuity-review-link", payload.links?.pre_session_review || null);

  const target = document.getElementById("continuity-timeline");
  target.innerHTML = "";
  const entries = payload.entries || [];
  if (!entries.length) {
    target.innerHTML = "<li>No continuity timeline data yet.</li>";
    return;
  }

  entries.forEach((entry) => {
    const item = document.createElement("li");
    const details = (entry.details || []).map((detail) => escapeHtml(detail)).join("<br />");
    item.innerHTML = `
      <div><strong>${escapeHtml(entry.title || "-")}</strong> <span class="status-${timelineStatusLevel(entry.status)}">${escapeHtml(String(entry.status || "-").toUpperCase())}</span></div>
      <div>${escapeHtml(entry.timestamp || "timestamp unavailable")}</div>
      <div>${details || "-"}</div>
    `;
    target.appendChild(item);
  });
}

function setButtonsBusy() {
  document.querySelectorAll("button[data-action]").forEach((button) => {
    if (!button.id.startsWith("manual-")) {
      button.disabled = state.actionInFlight || button.dataset.interlocked === "true";
    }
  });
}

function setActionOutput(payload) {
  state.lastAction = payload;
  text("action-name", payload.action_label || payload.action || "-");
  setActionStatus("action-status", payload.kind || "-");
  text("action-timestamp", payload.timestamp || "-");
  text("action-command", payload.command || "-");
  const fullOutput = [payload.message, payload.output]
    .filter(Boolean)
    .join("\n\n")
    .trim();
  text("action-output", summarizeActionOutput(payload.message || payload.output || "-"));
  const snippet = [payload.stdout_snippet, payload.stderr_snippet].filter(Boolean).join("\n");
  text("action-snippet", snippet || "-");
  text("action-full-output", fullOutput || "-");
}

function setBadge(id, label, level) {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = label;
  el.className = `badge ${badgeClass(level)}`;
}

function setStatusValue(id, label, level) {
  const el = document.getElementById(id);
  if (!el) return;
  const mono = el.classList.contains("mono");
  el.textContent = label;
  el.className = `value status-${level}${mono ? " mono" : ""}`;
}

function badgeClass(level) {
  if (level === "ok") return "badge badge-accent";
  if (level === "warning") return "badge badge-warning";
  if (level === "danger") return "badge badge-danger";
  if (level === "info") return "badge badge-info";
  return "badge badge-muted";
}

function contextStatusLevel(status) {
  const normalized = String(status || "").toLowerCase();
  if (normalized === "live") return "ok";
  if (normalized === "live_thin_comparison" || normalized === "live_no_valid_prior") return "info";
  if (normalized === "stale" || normalized === "degraded") return "warning";
  return "muted";
}

function statusLevelForChecklist(status) {
  const normalized = String(status || "").toLowerCase();
  if (normalized === "done") return "ok";
  if (normalized === "pending_optional") return "info";
  if (normalized === "blocked") return "danger";
  if (normalized === "pending") return "warning";
  return "muted";
}

function timelineStatusLevel(status) {
  const normalized = String(status || "").toLowerCase();
  if (normalized === "clean" || normalized === "reviewed" || normalized === "completed") return "ok";
  if (normalized === "active" || normalized === "pending" || normalized === "guarded" || normalized === "ready to clear") return "warning";
  if (normalized === "unresolved" || normalized === "blocked") return "danger";
  if (normalized === "missing" || normalized === "unknown" || normalized === "not required") return "muted";
  return "info";
}

function timelineBadgeClass(category) {
  const normalized = String(category || "").toLowerCase();
  if (normalized === "shape") return "badge badge-info";
  if (normalized === "branch") return "badge badge-accent";
  if (normalized === "signal") return "badge badge-info";
  if (normalized === "intent") return "badge badge-warning";
  if (normalized === "fill" || normalized === "trade") return "badge badge-accent";
  if (normalized === "block" || normalized === "reconciliation") return "badge badge-danger";
  if (normalized === "position") return "badge badge-warning";
  if (normalized === "operator") return "badge badge-warning";
  if (normalized === "risk" || normalized === "runtime") return "badge badge-danger";
  return "badge badge-muted";
}

function levelForMode(mode) {
  if (mode === "PAPER") return "ok";
  if (mode === "SHADOW") return "info";
  return "muted";
}

function levelForMarketData(label) {
  if (label === "LIVE") return "ok";
  if (label === "STALE") return "warning";
  if (label === "DEAD") return "danger";
  return "muted";
}

function levelForHealth(label) {
  if (label === "HEALTHY") return "ok";
  if (label === "DEGRADED") return "warning";
  if (label === "FAULT" || label === "FAULTED" || label === "UNHEALTHY") return "danger";
  return "muted";
}

function levelForCurveState(label) {
  const normalized = String(label || "").toUpperCase();
  if (normalized.includes("BULL") || normalized.includes("STABLE")) return "ok";
  if (normalized.includes("BEAR") || normalized.includes("VOLATILE") || normalized === "MIXED") return "warning";
  if (normalized === "INSUFFICIENT DATA") return "muted";
  return "info";
}

function classificationLevel(label) {
  const normalized = String(label || "").toLowerCase();
  if (normalized === "approved_quant") return "ok";
  if (normalized === "admitted_paper") return "info";
  if (normalized === "experimental_canary") return "warning";
  if (normalized === "canary") return "warning";
  return "muted";
}

function approvedModelChainLevel(label) {
  const normalized = String(label || "").toUpperCase();
  if (normalized === "FILLED_OPEN") return "warning";
  if (normalized === "FILLED_CLOSED") return "ok";
  if (normalized === "BLOCKED" || normalized === "INTENT_WITHOUT_FILL") return "danger";
  if (normalized === "DECISION_WITHOUT_INTENT") return "warning";
  if (normalized.includes("UNKNOWN")) return "muted";
  if (normalized === "NO_SIGNAL") return "muted";
  return "info";
}

function paperExceptionSeverityLevel(label) {
  const normalized = String(label || "").toUpperCase();
  if (normalized === "BLOCKING") return "danger";
  if (normalized === "ACTION") return "danger";
  if (normalized === "WATCH") return "warning";
  if (normalized === "INFO") return "info";
  return "muted";
}

function paperExceptionVerdictLevel(label) {
  const normalized = String(label || "").toUpperCase();
  if (normalized === "FAULTED" || normalized === "DIRTY_RECONCILIATION" || normalized === "FLATTEN_AND_HALT" || normalized === "HALTED_CATASTROPHIC") return "danger";
  if (normalized === "RUNNING_WITH_OPEN_RISK" || normalized === "HALTED_WITH_OPEN_RISK" || normalized === "NEEDS_OPERATOR_REVIEW" || normalized === "HALT_NEW_ENTRIES" || normalized === "HALTED_DEGRADATION" || normalized === "WATCH") return "warning";
  if (normalized === "RUNNING_CLEAN" || normalized === "CLEAN_IDLE" || normalized === "OK") return "ok";
  return "muted";
}

function paperSoakVerdictLevel(label) {
  const normalized = String(label || "").toUpperCase();
  if (normalized === "FAULTED_SESSION" || normalized === "DIRTY_AT_CLOSE") return "danger";
  if (normalized === "FILLED_WITH_OPEN_RISK" || normalized === "ACTIVITY_NO_FILL") return "warning";
  if (normalized === "FILLED_AND_FLAT") return "ok";
  if (normalized === "BLOCKED_SESSION") return "info";
  if (normalized === "NO_ACTIVITY") return "muted";
  return "muted";
}

function renderModelList(values) {
  return Array.isArray(values) && values.length ? values.join(", ") : "None";
}

function formatSpreadValue(payload) {
  if (!payload || !payload.current_bp) return "Unavailable";
  const change = payload.day_change_bp ? ` (${payload.day_change_bp} bp DoD)` : "";
  return `${payload.current_bp} bp${change}`;
}

function svgLine(x1, y1, x2, y2, stroke, strokeWidth) {
  const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
  line.setAttribute("x1", String(x1));
  line.setAttribute("y1", String(y1));
  line.setAttribute("x2", String(x2));
  line.setAttribute("y2", String(y2));
  line.setAttribute("stroke", stroke);
  line.setAttribute("stroke-width", String(strokeWidth));
  return line;
}

function svgText(x, y, content, fontSize, fill, anchor = "start") {
  const textNode = document.createElementNS("http://www.w3.org/2000/svg", "text");
  textNode.setAttribute("x", String(x));
  textNode.setAttribute("y", String(y));
  textNode.setAttribute("fill", fill);
  textNode.setAttribute("font-size", fontSize);
  textNode.setAttribute("text-anchor", anchor);
  textNode.textContent = content;
  return textNode;
}

function svgCircle(x, y, radius, fill) {
  const circle = document.createElementNS("http://www.w3.org/2000/svg", "circle");
  circle.setAttribute("cx", String(x));
  circle.setAttribute("cy", String(y));
  circle.setAttribute("r", String(radius));
  circle.setAttribute("fill", fill);
  return circle;
}

function setLink(id, href) {
  const el = document.getElementById(id);
  if (!el) return;
  if (!href) {
    el.href = "#";
    el.classList.add("disabled");
    el.setAttribute("aria-disabled", "true");
    return;
  }
  el.href = href;
  el.classList.remove("disabled");
  el.removeAttribute("aria-disabled");
}

function text(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = value;
}

function setActionStatus(id, value) {
  const el = document.getElementById(id);
  if (!el) return;
  const normalized = String(value || "-").toLowerCase();
  let level = "muted";
  if (normalized === "ok" || normalized === "completed" || normalized === "success") {
    level = "ok";
  } else if (normalized === "pending") {
    level = "warning";
  } else if (normalized === "failed") {
    level = "danger";
  }
  el.textContent = String(value || "-").toUpperCase();
  el.className = `value status-${level}`;
}

function setSideValue(id, side) {
  const el = document.getElementById(id);
  if (!el) return;
  const normalized = String(side || "FLAT").toUpperCase();
  let levelClass = "flat";
  if (normalized === "LONG") {
    levelClass = "long";
  } else if (normalized === "SHORT") {
    levelClass = "short";
  }
  el.textContent = normalized;
  el.className = `hero-side ${levelClass}`;
}

function setPnlValue(id, value, fallback) {
  const el = document.getElementById(id);
  if (!el) return;
  const resolved = value ?? fallback;
  el.textContent = resolved;
  el.classList.remove("pnl-positive", "pnl-negative", "pnl-flat");
  const numeric = Number.parseFloat(String(value ?? "").replaceAll(",", ""));
  if (!Number.isFinite(numeric)) {
    el.classList.add("pnl-flat");
    return;
  }
  if (numeric > 0) {
    el.classList.add("pnl-positive");
  } else if (numeric < 0) {
    el.classList.add("pnl-negative");
  } else {
    el.classList.add("pnl-flat");
  }
}

function setSignedValue(id, value) {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = value ?? "-";
  el.classList.remove("pnl-positive", "pnl-negative", "pnl-flat");
  const firstPart = String(value ?? "").split("/")[0].trim();
  const numeric = Number.parseFloat(firstPart.replaceAll(",", ""));
  if (!Number.isFinite(numeric)) {
    el.classList.add("pnl-flat");
    return;
  }
  if (numeric > 0) {
    el.classList.add("pnl-positive");
  } else if (numeric < 0) {
    el.classList.add("pnl-negative");
  } else {
    el.classList.add("pnl-flat");
  }
}

function summarizeActionOutput(textValue) {
  const normalized = String(textValue || "-").trim();
  if (normalized.length <= 220) {
    return normalized;
  }
  return `${normalized.slice(0, 217)}...`;
}

function marketFeedLevel(state) {
  const normalized = String(state || "").toUpperCase();
  if (normalized === "LIVE") return "ok";
  if (normalized === "DELAYED" || normalized === "PARTIAL" || normalized === "STALE") return "warning";
  if (normalized === "UNAVAILABLE") return "danger";
  return "muted";
}

function renderContributionCard(valueId, noteId, payload) {
  if (!payload || !payload.branch) {
    text(valueId, "Unavailable");
    text(noteId, "No clear latest-session attribution.");
    return;
  }
  text(valueId, `${payload.branch} • ${payload.total_contribution || "N/A"}`);
  text(noteId, [payload.net_effect, payload.timing_hint, payload.path_hint].filter(Boolean).join(" • ") || "-");
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function yesNo(value) {
  return value ? "Yes" : "No";
}

document.querySelectorAll("button[data-action]").forEach((button) => {
  button.addEventListener("click", () => {
    warmPaperFillAudioContext();
    const action = button.dataset.action;
    if (!action || button.disabled) return;
    runAction(action);
  });
});

document.addEventListener("pointerdown", () => {
  warmPaperFillAudioContext();
}, { passive: true });

document.getElementById("refresh-interval").addEventListener("change", (event) => {
  state.refreshIntervalSeconds = Number(event.target.value);
  scheduleRefresh();
});

const approvedModelSelect = document.getElementById("approved-model-select");
if (approvedModelSelect) {
  approvedModelSelect.addEventListener("change", (event) => {
    state.selectedApprovedModel = event.target.value || null;
    render();
  });
}

[
  ["blotter", "blotter-filter"],
  ["fills", "fills-filter"],
  ["intents", "intents-filter"],
  ["historicalPlayback", "historical-playback-filter"],
  ["branchSession", "branch-session-filter"],
  ["branchHistory", "branch-history-filter"],
  ["recent", "recent-trades-filter"],
  ["sessionHistory", "session-history-filter"],
].forEach(([name, inputId]) => {
  const input = document.getElementById(inputId);
  if (!input) return;
  input.addEventListener("input", (event) => {
    state.filters[name] = event.target.value;
    render();
  });
});

const paperFillDesktopToggle = document.getElementById("paper-fill-desktop-toggle");
if (paperFillDesktopToggle) {
  paperFillDesktopToggle.addEventListener("click", () => {
    warmPaperFillAudioContext();
    togglePaperFillDesktopAlerts();
  });
}

const paperFillSoundToggle = document.getElementById("paper-fill-sound-toggle");
if (paperFillSoundToggle) {
  paperFillSoundToggle.addEventListener("click", () => {
    warmPaperFillAudioContext();
    togglePaperFillSoundAlerts();
  });
}

const paperFillNotificationRequest = document.getElementById("paper-fill-notification-request");
if (paperFillNotificationRequest) {
  paperFillNotificationRequest.addEventListener("click", () => {
    warmPaperFillAudioContext();
    requestPaperFillNotificationPermission();
  });
}

fetchDashboard()
  .then(() => runAction("auth-gate-check"))
  .catch((error) => {
    setActionOutput({
      action_label: "Dashboard Load",
      kind: "failed",
      timestamp: new Date().toISOString(),
      message: String(error),
      command: null,
      stdout_snippet: "",
      stderr_snippet: String(error),
      output: String(error),
    });
    render();
  });
