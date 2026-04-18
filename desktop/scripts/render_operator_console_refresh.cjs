const { app, BrowserWindow } = require("electron");
const fs = require("fs");
const path = require("path");

const desktopRoot = path.resolve(__dirname, "..");
const repoRoot = path.resolve(desktopRoot, "..");
const rendererDir = path.join(desktopRoot, "dist", "renderer");
const outputDir = path.join(repoRoot, "outputs", "operator_console_refresh");
const mockHtmlPath = path.join(rendererDir, "mock-index.html");
const harnessLogPath = path.join(outputDir, "renderer_harness.log");
const desktopStateFixturePath = process.env.MGC_RENDER_DESKTOP_STATE_FIXTURE_PATH
  ? path.resolve(process.env.MGC_RENDER_DESKTOP_STATE_FIXTURE_PATH)
  : null;

function logStep(message) {
  fs.appendFileSync(harnessLogPath, `[${new Date().toISOString()}] ${message}\n`);
}

function readJson(relativePath, fallback = {}) {
  const filePath = path.join(repoRoot, relativePath);
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch {
    return fallback;
  }
}

function readJsonAbsolute(filePath, fallback = {}) {
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch {
    return fallback;
  }
}

function buildCalendarTradeLogSeed() {
  const rows = [
    ["2026-04-01T11:32:00-04:00", "mgc_us_late_pause_resume_long", "mgc_us_late_pause_resume_long__MGC", "MGC / usLatePauseResumeLongTurn", 12],
    ["2026-04-02T15:20:00-04:00", "atp_companion_v1_gc_asia_promotion_1_075r_favorable_only", "atp_companion_v1__paper_gc_asia__promotion_1_075r_favorable_only", "ATP Companion Candidate — GC / Asia Only / Promotion 1 +0.75R Favorable Only", -8],
    ["2026-04-03T10:18:00-04:00", "gc_asia_early_normal_breakout_retest_hold_long", "gc_asia_early_normal_breakout_retest_hold_long__GC", "GC / asiaEarlyNormalBreakoutRetestHoldTurn", 14],
    ["2026-04-06T11:08:00-04:00", "atp_companion_v1_asia_us", "atp_companion_v1__benchmark_mgc_asia_us", "ATP Companion Baseline v1 — Asia + US Executable, London Diagnostic-Only", -12],
    ["2026-04-07T13:45:00-04:00", "mgc_asia_early_normal_breakout_retest_hold_long", "mgc_asia_early_normal_breakout_retest_hold_long__MGC", "MGC / asiaEarlyNormalBreakoutRetestHoldTurn", 25],
    ["2026-04-08T15:05:00-04:00", "atp_companion_v1_pl_asia_us", "atp_companion_v1__paper_pl_asia_us", "ATP Companion Candidate v1 — PL / Asia + US Executable, London Diagnostic-Only", 50],
    ["2026-04-09T14:22:00-04:00", "pl_us_late_pause_resume_long", "pl_us_late_pause_resume_long__PL", "PL / usLatePauseResumeLongTurn", -11],
    ["2026-04-10T12:12:00-04:00", "gc_asia_early_normal_breakout_retest_hold_long", "gc_asia_early_normal_breakout_retest_hold_long__GC", "GC / asiaEarlyNormalBreakoutRetestHoldTurn", 72],
    ["2026-04-13T09:55:00-04:00", "atp_companion_v1_mgc_asia_promotion_1_075r_favorable_only", "atp_companion_v1__paper_mgc_asia__promotion_1_075r_favorable_only", "ATP Companion Candidate — MGC / Asia Only / Promotion 1 +0.75R Favorable Only", 29],
    ["2026-04-14T14:35:00-04:00", "gc_asia_early_normal_breakout_retest_hold_long", "gc_asia_early_normal_breakout_retest_hold_long__GC", "GC / asiaEarlyNormalBreakoutRetestHoldTurn", 70],
    ["2026-04-15T13:20:00-04:00", "mgc_us_late_pause_resume_long", "mgc_us_late_pause_resume_long__MGC", "MGC / usLatePauseResumeLongTurn", 0],
    ["2026-04-16T11:48:00-04:00", "atp_companion_v1_gc_asia_us", "atp_companion_v1__paper_gc_asia_us", "ATP Companion Candidate v1 — GC / Asia + US Executable, London Diagnostic-Only", 123],
    ["2026-04-17T15:07:00-04:00", "atp_companion_v1_asia_us", "atp_companion_v1__benchmark_mgc_asia_us", "ATP Companion Baseline v1 — Asia + US Executable, London Diagnostic-Only", -17],
    ["2026-04-20T11:15:00-04:00", "mgc_asia_early_normal_breakout_retest_hold_long", "mgc_asia_early_normal_breakout_retest_hold_long__MGC", "MGC / asiaEarlyNormalBreakoutRetestHoldTurn", 102],
    ["2026-04-21T12:40:00-04:00", "gc_asia_early_normal_breakout_retest_hold_long", "gc_asia_early_normal_breakout_retest_hold_long__GC", "GC / asiaEarlyNormalBreakoutRetestHoldTurn", 50],
    ["2026-04-22T10:50:00-04:00", "atp_companion_v1_mgc_asia_promotion_1_075r_favorable_only", "atp_companion_v1__paper_mgc_asia__promotion_1_075r_favorable_only", "ATP Companion Candidate — MGC / Asia Only / Promotion 1 +0.75R Favorable Only", 17],
    ["2026-04-23T14:05:00-04:00", "atp_companion_v1_gc_asia_us", "atp_companion_v1__paper_gc_asia_us", "ATP Companion Candidate v1 — GC / Asia + US Executable, London Diagnostic-Only", 77],
    ["2026-04-24T15:02:00-04:00", "pl_us_late_pause_resume_long", "pl_us_late_pause_resume_long__PL", "PL / usLatePauseResumeLongTurn", -33],
    ["2026-04-27T11:18:00-04:00", "atp_companion_v1_pl_asia_us", "atp_companion_v1__paper_pl_asia_us", "ATP Companion Candidate v1 — PL / Asia + US Executable, London Diagnostic-Only", -21],
    ["2026-04-28T14:45:00-04:00", "atp_companion_v1_asia_us", "atp_companion_v1__benchmark_mgc_asia_us", "ATP Companion Baseline v1 — Asia + US Executable, London Diagnostic-Only", -18],
    ["2026-04-29T13:10:00-04:00", "mgc_us_late_pause_resume_long", "mgc_us_late_pause_resume_long__MGC", "MGC / usLatePauseResumeLongTurn", -14],
    ["2026-04-30T15:33:00-04:00", "atp_companion_v1_gc_asia_promotion_1_075r_favorable_only", "atp_companion_v1__paper_gc_asia__promotion_1_075r_favorable_only", "ATP Companion Candidate — GC / Asia Only / Promotion 1 +0.75R Favorable Only", 4],
  ];
  return rows.map(([exitTimestamp, laneId, standaloneStrategyId, strategyName, realizedPnl], index) => ({
    id: `${laneId}:${index + 1}`,
    trade_id: index + 1,
    lane_id: laneId,
    standalone_strategy_id: standaloneStrategyId,
    strategy_key: standaloneStrategyId,
    strategy_name: strategyName,
    instrument: standaloneStrategyId.includes("__GC") ? "GC" : standaloneStrategyId.includes("__PL") ? "PL" : "MGC",
    side: realizedPnl >= 0 ? "LONG" : "SHORT",
    status: "CLOSED",
    entry_timestamp: exitTimestamp,
    exit_timestamp: exitTimestamp,
    realized_pnl: String(realizedPnl),
    gross_pnl: String(realizedPnl),
    quantity: 1,
    paper_strategy_class: laneId.startsWith("atp_") ? "temporary_paper_strategy" : "approved_or_admitted_paper_strategy",
  }));
}

function buildDashboardPayload() {
  const operatorSurface = readJson("outputs/operator_dashboard/operator_surface_snapshot.json");
  const paperReadiness = readJson("outputs/operator_dashboard/paper_readiness_snapshot.json");
  const approvedModels = readJson("outputs/operator_dashboard/paper_approved_models_snapshot.json");
  const strategyPerformance = readJson("outputs/operator_dashboard/paper_strategy_performance_snapshot.json");
  if (!Array.isArray(strategyPerformance.trade_log) || strategyPerformance.trade_log.length < 12) {
    strategyPerformance.trade_log = buildCalendarTradeLogSeed();
  }
  const nonApproved = readJson("outputs/operator_dashboard/paper_non_approved_lanes_snapshot.json");
  const tempIntegrity = readJson("outputs/operator_dashboard/paper_temporary_paper_runtime_integrity_snapshot.json");
  const productionLink = readJson("outputs/operator_dashboard/production_link_snapshot.json");
  const strategyAnalysis = readJson("outputs/operator_dashboard/strategy_analysis_snapshot.json");
  const playback = readJson("outputs/operator_dashboard/historical_playback_snapshot.json");
  const researchCapture = readJson("outputs/operator_dashboard/research_daily_capture_status.json");
  const actionLog = readJson("outputs/operator_dashboard/action_log.jsonl", null);
  return {
    global: {
      mode_label: "SHARED PAPER / LOCAL OPERATOR",
      entries_enabled: paperReadiness.entries_enabled === true,
      auth_label: "READY",
      market_data_label: "READY",
      runtime_health_label: "HEALTHY",
      current_session_date: paperReadiness.current_detected_session ?? "UNCLASSIFIED",
      last_update_timestamp: operatorSurface.generated_at ?? new Date().toISOString(),
      paper_running: String(paperReadiness.runtime_phase ?? "").toUpperCase() === "RUNNING",
    },
    operator_surface: operatorSurface,
    paper: {
      running: String(paperReadiness.runtime_phase ?? "").toUpperCase() === "RUNNING",
      readiness: paperReadiness,
      approved_models: approvedModels,
      strategy_performance: strategyPerformance,
      non_approved_lanes: nonApproved,
      temporary_paper_runtime_integrity: tempIntegrity,
      runtime_registry: {
        rows: approvedModels.rows ?? [],
        summary: {
          configured_standalone_strategies: Array.isArray(approvedModels.rows) ? approvedModels.rows.length : 0,
          runtime_instances_present: Array.isArray(approvedModels.rows) ? approvedModels.rows.length : 0,
          runtime_states_loaded: Array.isArray(approvedModels.rows) ? approvedModels.rows.length : 0,
          can_process_bars: Array.isArray(approvedModels.rows) ? approvedModels.rows.length : 0,
        },
      },
      signal_intent_fill_audit: readJson("outputs/operator_dashboard/paper_signal_intent_fill_audit_snapshot.json"),
      events: { alerts: [] },
      alerts_state: { active_alerts: [] },
      runtime_recovery: { status: "RUNNING", manual_action_required: false },
      soak_continuity: { healthy_soak: true },
      broker_truth_shadow_validation: readJson("outputs/operator_dashboard/paper_broker_truth_shadow_validation_snapshot.json"),
      live_timing_summary: readJson("outputs/operator_dashboard/paper_live_timing_summary_snapshot.json"),
      live_timing_validation: readJson("outputs/operator_dashboard/paper_live_timing_validation_snapshot.json"),
      soak_validation: readJson("outputs/operator_dashboard/paper_soak_validation_snapshot.json"),
      soak_extended: readJson("outputs/operator_dashboard/paper_soak_extended_snapshot.json"),
      soak_unattended: readJson("outputs/operator_dashboard/paper_soak_unattended_snapshot.json"),
      exit_parity_summary: readJson("outputs/operator_dashboard/paper_exit_parity_summary_snapshot.json"),
      strategy_runtime_summary: {
        configured_standalone_strategies: Array.isArray(approvedModels.rows) ? approvedModels.rows.length : 0,
        runtime_instances_present: Array.isArray(approvedModels.rows) ? approvedModels.rows.length : 0,
        runtime_states_loaded: Array.isArray(approvedModels.rows) ? approvedModels.rows.length : 0,
        can_process_bars: Array.isArray(approvedModels.rows) ? approvedModels.rows.length : 0,
        in_position_strategies: 0,
        strategies_with_faults_or_blockers: 0,
        same_underlying_ambiguity_count: 0,
        generated_at: operatorSurface.generated_at ?? new Date().toISOString(),
      },
      raw_operator_status: {
        updated_at: operatorSurface.generated_at ?? new Date().toISOString(),
      },
    },
    production_link: productionLink,
    strategy_analysis: strategyAnalysis,
    historical_playback: playback,
    research_capture: researchCapture,
    same_underlying_conflicts: {
      rows: [],
      summary: {},
      notes: [],
      events: { rows: [], summary: {} },
    },
    shadow: {
      raw_operator_status: {},
    },
    __action_log_hint: actionLog,
  };
}

function buildDesktopState() {
  if (desktopStateFixturePath) {
    return readJsonAbsolute(desktopStateFixturePath, {});
  }
  const dashboard = buildDashboardPayload();
  return {
    connection: "live",
    dashboard,
    health: { status: "ok", ready: true, checks: {} },
    backendUrl: "http://127.0.0.1:8790",
    source: {
      mode: "live_api",
      label: "LIVE API",
      detail: "Mocked live operator source for renderer validation.",
      canRunLiveActions: true,
      healthReachable: true,
      apiReachable: true,
    },
    backend: {
      state: "healthy",
      label: "HEALTHY",
      detail: "Mocked backend/API attached for renderer acceptance validation.",
      lastError: null,
      nextRetryAt: null,
      retryCount: 0,
      pid: 15144,
      apiStatus: "responding",
      healthStatus: "ok",
      managerOwned: false,
      startupFailureKind: "none",
      actionHint: null,
      staleListenerDetected: false,
      healthReachable: true,
      dashboardApiTimedOut: false,
      portConflictDetected: false,
    },
    startup: {
      preferredHost: "127.0.0.1",
      preferredPort: 8790,
      preferredUrl: "http://127.0.0.1:8790/",
      allowPortFallback: false,
      chosenHost: "127.0.0.1",
      chosenPort: 8790,
      chosenUrl: "http://127.0.0.1:8790/",
      mode: "SERVICE_ATTACHED",
      ownership: "attached_existing",
      latestEvent: "Renderer mock harness attached.",
      recentEvents: ["Renderer mock harness attached."],
      failureKind: "none",
      recommendedAction: null,
      staleListenerDetected: false,
      healthReachable: true,
      dashboardApiTimedOut: false,
      managedExitCode: null,
      managedExitSignal: null,
    },
    infoFiles: [],
    errors: [],
    runtimeLogPath: path.join(repoRoot, "outputs", "probationary_pattern_engine", "paper_session", "runtime.log"),
    backendLogPath: path.join(repoRoot, "outputs", "operator_dashboard", "runtime", "operator_dashboard.log"),
    desktopLogPath: path.join(repoRoot, "desktop", "logs", "mock.log"),
    appVersion: "operator-refresh-mock",
    manager: {
      running: true,
      lastExitCode: null,
      lastExitSignal: null,
      recentOutput: ["Renderer mock harness started."],
    },
    localAuth: {
      auth_available: true,
      auth_platform: "macos",
      auth_method: "TOUCH_ID",
      last_authenticated_at: new Date().toISOString(),
      last_auth_result: "SUCCEEDED",
      last_auth_detail: "Mock local operator auth is active.",
      auth_session_expires_at: new Date(Date.now() + 30 * 60 * 1000).toISOString(),
      auth_session_active: true,
      local_operator_identity: "patrick",
      auth_session_id: "mock-session",
      touch_id_available: true,
      secret_protection: {
        available: true,
        provider: "KEYCHAIN_SAFE_STORAGE",
        wrapper_ready: true,
        wrapper_path: "/tmp/mock-wrapper",
        protects_token_file_directly: false,
        detail: "Mocked secure wrapper.",
      },
      latest_event: null,
      recent_events: [],
      artifacts: {
        state_path: "/tmp/mock-local-auth-state.json",
        events_path: "/tmp/mock-local-auth-events.jsonl",
        secret_wrapper_path: "/tmp/mock-wrapper",
      },
    },
    refreshedAt: new Date().toISOString(),
  };
}

function writeMockHtml(state) {
  const template = fs.readFileSync(path.join(rendererDir, "index.html"), "utf8");
  const injection = `
    <script>
      window.__DESKTOP_STATE__ = ${JSON.stringify(state)};
      window.confirm = () => true;
      window.operatorDesktop = {
        async getDesktopState() { return window.__DESKTOP_STATE__; },
        async startDashboard() { return { ok: true, message: "Dashboard/API running", detail: "Mock startup path is healthy." }; },
        async stopDashboard() { return { ok: true, message: "Dashboard/API stopped", detail: "Mock stop completed." }; },
        async restartDashboard() { return { ok: true, message: "Dashboard/API restarted", detail: "Mock restart completed." }; },
        async runDashboardAction(action) {
          const messages = {
            "start-paper": ["Paper runtime started", "Shared runtime start path succeeded."],
            "restart-paper-with-temp-paper": ["Paper runtime restarted", "Shared restart path succeeded and temp-paper stayed aligned."],
            "stop-paper": ["Paper runtime stopped", "Shared stop path succeeded."],
            "auth-gate-check": ["Auth gate ready", "Shared auth truth is green."],
          };
          const tuple = messages[action] || [String(action), "Mock dashboard action completed."];
          return { ok: true, message: tuple[0], detail: tuple[1], output: "" };
        },
        async runProductionLinkAction(action) {
          return { ok: true, message: "Production-link action completed", detail: "Mocked production-link action: " + action, output: "" };
        },
        async authenticateLocalOperator() { return { ok: true, message: "Local operator authenticated", detail: "Mock Touch ID success." }; },
        async clearLocalOperatorAuthSession() { return { ok: true, message: "Local auth cleared", detail: "Mock session cleared." }; },
        async openPath(targetPath) { return { ok: true, message: "Opened path", detail: targetPath }; },
        async openExternalUrl(targetUrl) { return { ok: true, message: "Opened URL", detail: targetUrl }; },
        async copyText() { return { ok: true, message: "Copied diagnostics", detail: "Mock clipboard copy succeeded." }; },
      };
    </script>
  `;
  const html = template.replace("</head>", `${injection}\n</head>`);
  fs.writeFileSync(mockHtmlPath, html);
}

async function waitFor(windowRef, predicate) {
  const started = Date.now();
  while (Date.now() - started < 5000) {
    const ok = await windowRef.webContents.executeJavaScript(predicate);
    if (ok) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`Timed out waiting for predicate: ${predicate}`);
}

async function click(windowRef, selector) {
  await windowRef.webContents.executeJavaScript(`
    (() => {
      const node = document.querySelector(${JSON.stringify(selector)});
      if (!node) return false;
      node.click();
      return true;
    })()
  `);
}

async function clickButtonByText(windowRef, label) {
  const clicked = await windowRef.webContents.executeJavaScript(`
    (() => {
      const text = ${JSON.stringify(label)};
      const buttons = Array.from(document.querySelectorAll("button"));
      const node = buttons.find((button) => button.textContent?.trim() === text && !button.disabled);
      if (!node) return false;
      node.click();
      return true;
    })()
  `);
  if (!clicked) {
    throw new Error(`Unable to click button with label: ${label}`);
  }
}

async function clickElementContainingText(windowRef, selector, label) {
  const clicked = await windowRef.webContents.executeJavaScript(`
    (() => {
      const text = ${JSON.stringify(label)};
      const nodes = Array.from(document.querySelectorAll(${JSON.stringify(selector)}));
      const node = nodes.find((item) => (item.textContent || "").includes(text));
      if (!node) return false;
      node.click();
      return true;
    })()
  `);
  if (!clicked) {
    throw new Error(`Unable to click element matching ${selector} with text: ${label}`);
  }
}

async function readText(windowRef, selector) {
  return windowRef.webContents.executeJavaScript(`
    (() => {
      const node = document.querySelector(${JSON.stringify(selector)});
      return node ? (node.textContent || "").trim() : "";
    })()
  `);
}

async function scrollSectionIntoView(windowRef, title) {
  const found = await windowRef.webContents.executeJavaScript(`
    (() => {
      const text = ${JSON.stringify(title)};
      const sections = Array.from(document.querySelectorAll("[data-section-title]"));
      const node = sections.find((item) => item.getAttribute("data-section-title") === text);
      if (!node) return false;
      node.scrollIntoView({ block: "start", behavior: "instant" });
      window.scrollBy(0, -72);
      return true;
    })()
  `);
  if (!found) {
    throw new Error(`Unable to locate section title: ${title}`);
  }
}

async function scrollSelectorIntoView(windowRef, selector) {
  const found = await windowRef.webContents.executeJavaScript(`
    (() => {
      const node = document.querySelector(${JSON.stringify(selector)});
      if (!node) return false;
      node.scrollIntoView({ block: "start", behavior: "instant" });
      window.scrollBy(0, -48);
      return true;
    })()
  `);
  if (!found) {
    throw new Error(`Unable to locate selector: ${selector}`);
  }
}

async function capture(windowRef, pageId, sectionTitle, filename) {
  await click(windowRef, `[data-page="${pageId}"]`);
  await waitFor(windowRef, `document.querySelector('[data-page="${pageId}"]')?.classList.contains('active') === true`);
  await captureCurrent(windowRef, sectionTitle, filename);
}

async function captureCurrent(windowRef, sectionTitle, filename) {
  await scrollSectionIntoView(windowRef, sectionTitle);
  await new Promise((resolve) => setTimeout(resolve, 250));
  const image = await windowRef.capturePage();
  fs.writeFileSync(path.join(outputDir, filename), image.toPNG());
}

async function main() {
  fs.mkdirSync(outputDir, { recursive: true });
  fs.writeFileSync(harnessLogPath, "");
  const desktopState = buildDesktopState();
  writeMockHtml(desktopState);
  logStep("mock html written");

  await app.whenReady();
  const win = new BrowserWindow({
    width: 1600,
    height: 1240,
    show: false,
    backgroundColor: "#07111d",
    webPreferences: {
      contextIsolation: false,
      nodeIntegration: false,
    },
  });

  await win.loadFile(mockHtmlPath, { hash: "/home" });
  await waitFor(win, "document.querySelector('.operator-app') !== null");
  logStep("renderer loaded");

  await click(win, "button[data-page='home']");
  await waitFor(win, "document.querySelector('.page-eyebrow')?.textContent?.includes('Dashboard') === true");
  logStep("dashboard active");
  const visibleAtpCards = await win.webContents.executeJavaScript(`
    (() => Array.from(document.querySelectorAll(".strategy-card"))
      .map((node) => (node.textContent || "").trim())
      .filter((text) => text.includes("ATP Companion"))
      .slice(0, 8))()
  `);
  logStep(`visible ATP cards: ${JSON.stringify(visibleAtpCards)}`);

  await capture(win, "home", "Control Center", "dashboard.png");
  await capture(win, "calendar", "P&L Calendar", "pnl-calendar-monthly.png");
  await click(win, '[data-calendar-view-toggle="line"]');
  await waitFor(win, "document.querySelector('[data-calendar-view=\"line\"]') !== null");
  await captureCurrent(win, "P&L Calendar", "pnl-calendar-line.png");
  await click(win, '[data-calendar-view-toggle="calendar"]');
  await waitFor(win, "document.querySelector('[data-calendar-surface=\"monthly\"]') !== null");
  await click(win, '[data-calendar-day=\"2026-04-16\"]');
  await waitFor(win, "document.querySelector('[data-selected-calendar-day=\"2026-04-16\"]') !== null");
  await scrollSelectorIntoView(win, '[data-selected-calendar-day=\"2026-04-16\"]');
  await new Promise((resolve) => setTimeout(resolve, 250));
  const detailImage = await win.capturePage();
  fs.writeFileSync(path.join(outputDir, "pnl-calendar-day-detail.png"), detailImage.toPNG());
  await clickElementContainingText(win, ".calendar-contribution-table tbody tr", "ATP Companion Candidate");
  await waitFor(win, "document.querySelector('[data-page=\"strategies\"]')?.classList.contains('active') === true");
  logStep("clicked calendar contribution row and opened strategy deep-dive");
  await capture(win, "positions", "Live P&L Workspace", "live-pnl.png");
  await capture(win, "market", "Trade Entry Workspace", "trade-entry.png");
  logStep("dashboard, calendar, live pnl, and trade entry screenshots captured");
  await click(win, "button[data-page='home']");
  await waitFor(win, "document.querySelector('[data-page=\"home\"]')?.classList.contains('active') === true");
  logStep("returned to dashboard for ATP click proof");

  await clickElementContainingText(win, ".strategy-card", "ATP Companion Candidate");
  await waitFor(win, "document.querySelector('[data-page=\"strategies\"]')?.classList.contains('active') === true");
  logStep("clicked ATP candidate card and opened strategy deep-dive");
  await capture(win, "strategies", "Strategy Deep-Dive Workspace", "strategy-deep-dive.png");
  logStep("strategy deep-dive screenshot captured after ATP card click");

  const acceptance = {
    generated_at: new Date().toISOString(),
    screenshots: {
      dashboard: path.join(outputDir, "dashboard.png"),
      pnl_calendar_monthly: path.join(outputDir, "pnl-calendar-monthly.png"),
      pnl_calendar_line: path.join(outputDir, "pnl-calendar-line.png"),
      pnl_calendar_day_detail: path.join(outputDir, "pnl-calendar-day-detail.png"),
      live_pnl: path.join(outputDir, "live-pnl.png"),
      trade_entry: path.join(outputDir, "trade-entry.png"),
      strategy_deep_dive: path.join(outputDir, "strategy-deep-dive.png"),
    },
    click_proof: {
      nav_dashboard: true,
      nav_pnl_calendar: true,
      nav_live_pnl: true,
      nav_trade_entry: true,
      nav_strategy_deep_dive: true,
      calendar_day_detail_opened: true,
      calendar_strategy_contribution_click_opened_deep_dive: true,
      visible_atp_cards: visibleAtpCards,
      open_strategy_navigation: "Strategy Deep-Dive",
      atp_experimental_click_opened_deep_dive: true,
    },
  };
  fs.writeFileSync(path.join(outputDir, "renderer_acceptance.json"), JSON.stringify(acceptance, null, 2));
  logStep("acceptance written");

  await win.close();
  if (fs.existsSync(mockHtmlPath)) {
    fs.unlinkSync(mockHtmlPath);
  }
  await app.quit();
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
