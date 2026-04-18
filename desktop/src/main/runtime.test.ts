import test from "node:test";
import assert from "node:assert/strict";

import { __testing, getDesktopState, prepareDesktopForLaunch, runDashboardAction, runProductionLinkAction, startDashboard, type DesktopState } from "./runtime";

function makeDesktopState(overrides: Partial<DesktopState> = {}): DesktopState {
  return {
    connection: "snapshot",
    dashboard: null,
    health: null,
    backendUrl: null,
    source: {
      mode: "snapshot_fallback",
      label: "STARTUP FAILURE / SNAPSHOT",
      detail: "Using persisted operator snapshots because Dashboard/API startup failed with stale listener conflict.",
      canRunLiveActions: false,
      healthReachable: false,
      apiReachable: false,
      ...(overrides.source ?? {}),
    },
    backend: {
      state: "backend_down",
      label: "STARTUP FAILURE",
      detail: "A stale listener conflict blocked the live dashboard API.",
      lastError: "STARTUP_FAILURE_KIND=stale_listener_conflict",
      nextRetryAt: null,
      retryCount: 0,
      pid: null,
      apiStatus: "timed_out",
      healthStatus: "unreachable",
      managerOwned: true,
      startupFailureKind: "stale_listener_conflict",
      actionHint: "Retry Dashboard/API start after stale-listener cleanup.",
      staleListenerDetected: true,
      healthReachable: false,
      dashboardApiTimedOut: true,
      portConflictDetected: true,
      ...(overrides.backend ?? {}),
    },
    startup: {
      preferredHost: "127.0.0.1",
      preferredPort: 8790,
      preferredUrl: "http://127.0.0.1:8790/",
      allowPortFallback: false,
      chosenHost: null,
      chosenPort: null,
      chosenUrl: null,
      mode: "SNAPSHOT_ONLY",
      ownership: "snapshot_only",
      latestEvent: null,
      recentEvents: [],
      failureKind: "stale_listener_conflict",
      recommendedAction: "Retry Dashboard/API start after stale-listener cleanup.",
      staleListenerDetected: true,
      healthReachable: false,
      dashboardApiTimedOut: true,
      managedExitCode: null,
      managedExitSignal: null,
      ...(overrides.startup ?? {}),
    } as DesktopState["startup"],
    infoFiles: [],
    errors: [],
    runtimeLogPath: null,
    backendLogPath: null,
    desktopLogPath: null,
    appVersion: "0.1.0",
    manager: {
      running: false,
      lastExitCode: null,
      lastExitSignal: null,
      recentOutput: [],
      ...(overrides.manager ?? {}),
    },
    localAuth: {
      auth_available: false,
      auth_platform: "macOS",
      auth_method: "NONE",
      last_authenticated_at: null,
      last_auth_result: "NONE",
      last_auth_detail: null,
      auth_session_expires_at: null,
      auth_session_ttl_seconds: 28800,
      auth_session_active: false,
      local_operator_identity: null,
      auth_session_id: null,
      touch_id_available: false,
      secret_protection: {
        available: false,
        provider: "NONE",
        wrapper_ready: false,
        wrapper_path: null,
        protects_token_file_directly: false,
        detail: "test",
      },
      latest_event: null,
      recent_events: [],
      artifacts: {
        state_path: "/tmp/local_operator_auth_state.json",
        events_path: "/tmp/local_operator_auth_events.jsonl",
        secret_wrapper_path: "/tmp/local_secret_wrapper.json",
      },
      ...(overrides.localAuth ?? {}),
    },
    refreshedAt: new Date().toISOString(),
    ...overrides,
  };
}

test("desktop dashboard recovery restores live API access and unlocks paper restart action", async () => {
  __testing.resetRuntimeState();

  const staleState = makeDesktopState();
  const reconnectingState = makeDesktopState({
    connection: "snapshot",
    source: {
      mode: "degraded_reconnecting",
      label: "RECOVERING",
      detail: "Managed backend recovery is active. Next reconnect attempt is scheduled.",
      canRunLiveActions: false,
      healthReachable: true,
      apiReachable: false,
    },
    backend: {
      state: "reconnecting",
      label: "RECOVERING",
      detail: "Next reconnect attempt scheduled.",
      lastError: "STARTUP_FAILURE_KIND=stale_listener_conflict\nSTARTUP_STALE_LISTENER_DETECTED=1",
      nextRetryAt: new Date(Date.now() + 1000).toISOString(),
      retryCount: 1,
      pid: 12345,
      apiStatus: "timed_out",
      healthStatus: "ok",
      managerOwned: true,
      startupFailureKind: "stale_listener_conflict",
      actionHint: "Retry Dashboard/API start after stale-listener cleanup.",
      staleListenerDetected: true,
      healthReachable: true,
      dashboardApiTimedOut: true,
      portConflictDetected: true,
    },
    startup: {
      preferredHost: "127.0.0.1",
      preferredPort: 8790,
      preferredUrl: "http://127.0.0.1:8790/",
      allowPortFallback: false,
      chosenHost: "127.0.0.1",
      chosenPort: 8790,
      chosenUrl: "http://127.0.0.1:8790/",
      mode: "DESKTOP_MANAGED_DIAGNOSTIC",
      ownership: "started_managed",
      latestEvent: "Recovering stale listener and waiting for /api/dashboard.",
      recentEvents: ["Recovering stale listener and waiting for /api/dashboard."],
      failureKind: "stale_listener_conflict",
      recommendedAction: "Retry Dashboard/API start after stale-listener cleanup.",
      staleListenerDetected: true,
      healthReachable: true,
      dashboardApiTimedOut: true,
      managedExitCode: null,
      managedExitSignal: null,
    },
  });
  const liveState = makeDesktopState({
    connection: "live",
    backendUrl: "http://127.0.0.1:8790/",
    health: { status: "ok", ready: true },
    source: {
      mode: "live_api",
      label: "LIVE API",
      detail: "Using live /health and /api/dashboard responses from the local dashboard server.",
      canRunLiveActions: true,
      healthReachable: true,
      apiReachable: true,
    },
    backend: {
      state: "healthy",
      label: "HEALTHY",
      detail: "Backend health and dashboard API are both responding.",
      lastError: null,
      nextRetryAt: null,
      retryCount: 0,
      pid: 12345,
      apiStatus: "responding",
      healthStatus: "ok",
      managerOwned: true,
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
      mode: "DESKTOP_MANAGED_DIAGNOSTIC",
      ownership: "started_managed",
      latestEvent: "Recovered stale listener and attached live backend.",
      recentEvents: ["Recovered stale listener and attached live backend."],
      failureKind: "none",
      recommendedAction: null,
      staleListenerDetected: false,
      healthReachable: true,
      dashboardApiTimedOut: false,
      managedExitCode: null,
      managedExitSignal: null,
    },
  });

  let currentState = staleState;
  __testing.setGetDesktopStateHook(async () => currentState);
  __testing.setBeginDashboardLaunchHook(async () => {
    currentState = reconnectingState;
    await new Promise((resolve) => setTimeout(resolve, 5));
    currentState = liveState;
    return liveState;
  });
  __testing.setFetchHook(async (input, init) => {
    assert.equal(String(input), "http://127.0.0.1:8790/api/action/restart-paper-with-temp-paper");
    assert.equal(init?.method, "POST");
    return new Response(
      JSON.stringify({
        ok: true,
        action_label: "Restart Runtime + Temp Paper",
        message: "Restarted paper runtime from the live desktop operator path.",
      }),
      {
        status: 200,
        headers: { "Content-Type": "application/json" },
      },
    );
  });

  const startResult = await startDashboard();
  assert.equal(startResult.ok, true);
  assert.equal(startResult.state?.connection, "live");
  assert.equal(startResult.state?.source.canRunLiveActions, true);
  assert.equal(startResult.state?.backend.apiStatus, "responding");

  const actionResult = await runDashboardAction("restart-paper-with-temp-paper");
  assert.equal(actionResult.ok, true);
  assert.equal(actionResult.message, "Restart Runtime + Temp Paper");
  assert.match(actionResult.detail ?? "", /Restarted paper runtime/);

  __testing.resetRuntimeState();
});

test("desktop state promotes to live when Node localhost transport is denied but curl fallback succeeds", async () => {
  __testing.resetRuntimeState();
  __testing.setBuildLocalOperatorAuthStateHook(async () => ({
    auth_available: false,
    auth_platform: "macOS",
    auth_method: "NONE",
    last_authenticated_at: null,
    last_auth_result: "NONE",
    last_auth_detail: null,
    auth_session_expires_at: null,
    auth_session_ttl_seconds: 28800,
    auth_session_active: false,
    local_operator_identity: null,
    auth_session_id: null,
    touch_id_available: false,
    secret_protection: {
      available: false,
      provider: "NONE",
      wrapper_ready: false,
      wrapper_path: null,
      protects_token_file_directly: false,
      detail: "test",
    },
    latest_event: null,
    recent_events: [],
    artifacts: {
      state_path: "/tmp/local_operator_auth_state.json",
      events_path: "/tmp/local_operator_auth_events.jsonl",
      secret_wrapper_path: "/tmp/local_secret_wrapper.json",
    },
  }));
  __testing.setFetchHook(async () => {
    const error = new TypeError("fetch failed") as TypeError & {
      cause?: { code: string; errno: number; syscall: string; address: string; port: number };
    };
    error.cause = {
      code: "EPERM",
      errno: 1,
      syscall: "connect",
      address: "127.0.0.1",
      port: 8790,
    };
    throw error;
  });
  __testing.setCurlJsonHook(async (url) => {
    if (url.endsWith("/health")) {
      return {
        status: "ok",
        ready: true,
        generated_at: new Date().toISOString(),
      };
    }
    return {
      dashboard_meta: {},
      global: { auth_ready: true },
      paper: {
        readiness: {},
        temporary_paper_runtime_integrity: { mismatch_status: "MATCHED" },
      },
      startup_control_plane: {
        overall_state: "READY",
        launch_allowed: true,
        convergence: {
          stable_ready: true,
          dashboard_attached: true,
          paper_runtime_ready: true,
        },
      },
      supervised_paper_operability: {
        app_usable_for_supervised_paper: true,
        state: "USABLE",
        summary_line: "Paper runtime is operational.",
        primary_next_action: { label: "Refresh" },
      },
    };
  });

  const state = await getDesktopState();

  assert.equal(state.connection, "live");
  assert.equal(state.source.mode, "live_api");
  assert.equal(state.source.healthReachable, true);
  assert.equal(state.source.apiReachable, true);
  assert.equal(state.backend.state, "healthy");
});

test("desktop state reports attached snapshot bridge when localhost transport is denied but readiness is healthy", async () => {
  __testing.resetRuntimeState();
  __testing.setBuildLocalOperatorAuthStateHook(async () => makeDesktopState().localAuth);
  __testing.setLoadSnapshotBundleHook(async () => ({
    generated_at: new Date().toISOString(),
    dashboard_meta: { server_instance_id: "instance-current" },
    global: { mode: "PAPER", mode_label: "PAPER", auth_ready: true, runtime_status: "RUNNING" },
    operator_surface: { generated_at: new Date().toISOString(), runtime_readiness: { values: {} } },
    paper: { readiness: { runtime_running: true, entries_enabled: true }, running: true },
    startup_control_plane: {},
    production_link: {},
  }));
  __testing.setLoadLiveDashboardHook(async () => null);
  __testing.setLoadAttachedSnapshotBridgeHook(async () => ({
    readiness: {},
    health: { status: "ok", ready: true },
    backendUrl: "http://127.0.0.1:8790/",
    detail: "Service is attached through the local readiness bridge and synchronized operator snapshot.",
  }));

  const state = await getDesktopState();

  assert.equal(state.connection, "snapshot");
  assert.equal(state.source.mode, "attached_snapshot_bridge");
  assert.equal(state.source.label, "SERVICE ATTACHED");
  assert.equal(state.backend.state, "healthy");
  assert.equal(state.source.canRunLiveActions, false);
  assert.equal(state.startup.mode, "SERVICE_ATTACHED");
  assert.deepEqual(state.errors, []);
});

test("packaged launch trusts a fresh synchronized local snapshot long enough to avoid transient fallback", async () => {
  __testing.resetRuntimeState();
  __testing.setPackagedLocalBundleLaunchContextHook(() => true);
  __testing.setBuildLocalOperatorAuthStateHook(async () => makeDesktopState().localAuth);

  let bootstrapCalls = 0;
  __testing.setEnsureServiceHostUsableHook(async () => {
    bootstrapCalls += 1;
  });
  __testing.setLoadSnapshotBundleHook(async () => ({
    generated_at: new Date(Date.now() - 90_000).toISOString(),
    dashboard_meta: {
      server_instance_id: "instance-current",
      server_pid: 42732,
      server_url: "http://127.0.0.1:8790/",
    },
    global: { mode: "PAPER", mode_label: "PAPER", auth_ready: true, runtime_status: "RUNNING" },
    operator_surface: { generated_at: new Date().toISOString(), runtime_readiness: { values: {} } },
    paper: { readiness: { runtime_running: true, entries_enabled: true }, running: true },
    startup_control_plane: {
      overall_state: "READY",
      launch_allowed: true,
      convergence: {
        stable_ready: true,
        dashboard_attached: true,
        paper_runtime_ready: true,
      },
    },
    supervised_paper_operability: {
      app_usable_for_supervised_paper: true,
      state: "USABLE",
      summary_line: "Paper runtime is operational.",
      primary_next_action: { label: "Refresh" },
    },
  }));
  __testing.setLoadAttachedSnapshotBridgeHook(async () => null);

  const state = await getDesktopState();

  assert.equal(bootstrapCalls, 0);
  assert.equal(state.connection, "snapshot");
  assert.equal(state.source.mode, "attached_snapshot_bridge");
  assert.equal(state.source.label, "SERVICE ATTACHED");
  assert.deepEqual(state.errors, []);
  __testing.resetRuntimeState();
});

test("recoverable reconnecting stale-listener state keeps waiting for live recovery", () => {
  __testing.resetRuntimeState();
  const reconnectingState = makeDesktopState({
    source: {
      mode: "degraded_reconnecting",
      label: "RECOVERING",
      detail: "Managed backend recovery is active.",
      canRunLiveActions: false,
      healthReachable: true,
      apiReachable: false,
    },
    backend: {
      state: "reconnecting",
      label: "RECOVERING",
      detail: "Waiting for live /api/dashboard after stale-listener cleanup.",
      lastError: "STARTUP_FAILURE_KIND=stale_listener_conflict\nSTARTUP_STALE_LISTENER_DETECTED=1",
      nextRetryAt: new Date(Date.now() + 1000).toISOString(),
      retryCount: 1,
      pid: 12345,
      apiStatus: "timed_out",
      healthStatus: "ok",
      managerOwned: true,
      startupFailureKind: "stale_listener_conflict",
      actionHint: "Retry Dashboard/API start after stale-listener cleanup.",
      staleListenerDetected: true,
      healthReachable: true,
      dashboardApiTimedOut: true,
      portConflictDetected: true,
    },
  });
  assert.equal(__testing.shouldContinueWaitingForRecovery(reconnectingState), true);
  __testing.resetRuntimeState();
});

test("health-only backend uses snapshot fallback immediately instead of blocking on service bootstrap", async () => {
  __testing.resetRuntimeState();

  let bootstrapCalls = 0;
  __testing.setLoadLiveDashboardHook(async () => ({
    mode: "health-only",
    url: "http://127.0.0.1:8790/",
    health: { status: "degraded", ready: false },
    error: "dashboard payload error",
  }));
  __testing.setLoadAttachedSnapshotBridgeHook(async () => null);
  __testing.setBuildLocalOperatorAuthStateHook(async () => makeDesktopState().localAuth);
  __testing.setLoadSnapshotBundleHook(async () => ({
    generated_at: new Date().toISOString(),
    global: { mode: "PAPER", mode_label: "PAPER", auth_ready: false, runtime_status: "DEGRADED" },
    operator_surface: { generated_at: new Date().toISOString(), runtime_readiness: { values: {} } },
    paper: { readiness: { runtime_running: false }, running: false },
    startup_control_plane: {},
    production_link: {},
  }));
  __testing.setEnsureServiceHostUsableHook(async () => {
    bootstrapCalls += 1;
    throw new Error("bootstrap should not run");
  });

  const state = await getDesktopState();

  assert.equal(bootstrapCalls, 0);
  assert.equal(state.connection, "snapshot");
  assert.equal(state.source.mode, "snapshot_fallback");
  assert.equal(state.backend.apiStatus, "timed_out");
  assert.match(state.errors[0] ?? "", /showing latest persisted operator snapshots/i);

  __testing.resetRuntimeState();
});

test("snapshot fallback with a stale backend endpoint starts automatic service recovery", async () => {
  __testing.resetRuntimeState();

  const previousSetting = process.env.MGC_DESKTOP_AUTO_BOOTSTRAP;
  process.env.MGC_DESKTOP_AUTO_BOOTSTRAP = "1";

  let bootstrapCalls = 0;
  let resolveBootstrap!: () => void;
  const bootstrapGate = new Promise<void>((resolve) => {
    resolveBootstrap = resolve;
  });

  __testing.setLoadLiveDashboardHook(async () => null);
  __testing.setBuildLocalOperatorAuthStateHook(async () => makeDesktopState().localAuth);
  __testing.setLoadAttachedSnapshotBridgeHook(async () => null);
  __testing.setLoadSnapshotBundleHook(async () => ({
    generated_at: new Date().toISOString(),
    global: { mode: "PAPER", mode_label: "PAPER", auth_ready: true, runtime_status: "RUNNING" },
    operator_surface: { generated_at: new Date().toISOString(), runtime_readiness: { values: {} } },
    paper: { readiness: { runtime_running: true, entries_enabled: true }, running: true },
    startup_control_plane: {},
    production_link: {},
  }));
  __testing.setEnsureServiceHostUsableHook(async () => {
    bootstrapCalls += 1;
    await bootstrapGate;
  });

  const state = await getDesktopState();

  assert.ok(bootstrapCalls >= 1);
  assert.equal(state.connection, "snapshot");
  assert.equal(state.source.mode, "degraded_reconnecting");
  assert.equal(state.backend.state, "starting");
  assert.match(state.source.detail, /(backend start is in progress|automatic backend recovery)/i);

  resolveBootstrap();
  await new Promise((resolve) => setTimeout(resolve, 0));
  if (previousSetting === undefined) {
    delete process.env.MGC_DESKTOP_AUTO_BOOTSTRAP;
  } else {
    process.env.MGC_DESKTOP_AUTO_BOOTSTRAP = previousSetting;
  }
  __testing.resetRuntimeState();
});

test("snapshot-first startup returns persisted state without waiting for live dashboard attach", { timeout: 4000 }, async () => {
  __testing.resetRuntimeState();

  __testing.setBuildLocalOperatorAuthStateHook(async () => makeDesktopState().localAuth);
  __testing.setLoadSnapshotBundleHook(async () => ({
    generated_at: new Date().toISOString(),
    global: { mode: "PAPER", mode_label: "PAPER", auth_ready: true, runtime_status: "RUNNING" },
    operator_surface: { generated_at: new Date().toISOString(), runtime_readiness: { values: {} } },
    paper: { readiness: { runtime_running: true, entries_enabled: true }, running: true },
    startup_control_plane: {},
    production_link: {},
  }));
  __testing.setLoadLiveDashboardHook(async () => await new Promise(() => {}));

  const startedAt = Date.now();
  const state = await Promise.race([
    getDesktopState(),
    new Promise<DesktopState>((_resolve, reject) => setTimeout(() => reject(new Error("getDesktopState timed out")), 2500)),
  ]);

  assert.equal(state.connection, "snapshot");
  assert.match(state.source.mode, /^(snapshot_fallback|degraded_reconnecting|attached_snapshot_bridge)$/);
  assert.ok(Date.now() - startedAt < 2200);
  __testing.resetRuntimeState();
});

test("snapshot-backed startup promotes to live API when the live dashboard responds shortly after launch", async () => {
  __testing.resetRuntimeState();

  __testing.setBuildLocalOperatorAuthStateHook(async () => makeDesktopState().localAuth);
  __testing.setLoadSnapshotBundleHook(async () => ({
    generated_at: new Date().toISOString(),
    global: { mode: "PAPER", mode_label: "PAPER", auth_ready: true, runtime_status: "RUNNING" },
    operator_surface: { generated_at: new Date().toISOString(), runtime_readiness: { values: {} } },
    paper: { readiness: { runtime_running: true, entries_enabled: true }, running: true },
    startup_control_plane: {},
    production_link: {},
  }));
  __testing.setLoadLiveDashboardHook(
    async () =>
      await new Promise((resolve) =>
        setTimeout(
          () =>
            resolve({
              mode: "live",
              url: "http://127.0.0.1:8790/",
              health: { status: "ok", ready: true },
              dashboard: {
                generated_at: new Date().toISOString(),
                global: { mode: "PAPER", mode_label: "PAPER", auth_ready: true, runtime_status: "RUNNING" },
                operator_surface: { generated_at: new Date().toISOString(), runtime_readiness: { values: {} } },
                paper: { readiness: { runtime_running: true, entries_enabled: true }, running: true },
                startup_control_plane: {
                  overall_state: "READY",
                  launch_allowed: true,
                  launch_candidate: true,
                  dependencies_aligned: true,
                },
                supervised_paper_operability: {
                  app_usable_for_supervised_paper: true,
                  summary_line: "Application is usable for supervised paper operation.",
                },
              },
            }),
          250,
        ),
      ),
  );

  const state = await getDesktopState();

  assert.equal(state.connection, "live");
  assert.equal(state.source.mode, "live_api");
  assert.equal(state.source.canRunLiveActions, true);
  __testing.resetRuntimeState();
});

test("sandboxed startup uses persisted snapshots without attempting automatic backend bootstrap", async () => {
  __testing.resetRuntimeState();

  const previousSetting = process.env.MGC_DESKTOP_AUTO_BOOTSTRAP;
  process.env.MGC_DESKTOP_AUTO_BOOTSTRAP = "0";

  let bootstrapCalls = 0;
  __testing.setBuildLocalOperatorAuthStateHook(async () => makeDesktopState().localAuth);
  __testing.setLoadAttachedSnapshotBridgeHook(async () => null);
  __testing.setLoadSnapshotBundleHook(async () => ({
    generated_at: new Date().toISOString(),
    global: { mode: "PAPER", mode_label: "PAPER", auth_ready: true, runtime_status: "RUNNING" },
    operator_surface: { generated_at: new Date().toISOString(), runtime_readiness: { values: {} } },
    paper: { readiness: { runtime_running: true, entries_enabled: true }, running: true },
    startup_control_plane: {},
    production_link: {},
  }));
  __testing.setLoadLiveDashboardHook(async () => null);
  __testing.setEnsureServiceHostUsableHook(async () => {
    bootstrapCalls += 1;
  });

  const state = await getDesktopState();

  assert.equal(bootstrapCalls, 0);
  assert.equal(state.connection, "snapshot");
  assert.equal(state.source.mode, "snapshot_fallback");

  if (previousSetting === undefined) {
    delete process.env.MGC_DESKTOP_AUTO_BOOTSTRAP;
  } else {
    process.env.MGC_DESKTOP_AUTO_BOOTSTRAP = previousSetting;
  }
  __testing.resetRuntimeState();
});

test("compact startup state strips heavyweight analytics payloads from persisted snapshots", async () => {
  __testing.resetRuntimeState();
  __testing.setBuildLocalOperatorAuthStateHook(async () => makeDesktopState().localAuth);
  __testing.setLoadSnapshotBundleHook(async () => ({
    generated_at: new Date().toISOString(),
    operator_surface: { generated_at: new Date().toISOString(), runtime_readiness: { values: {} } },
    paper: {
      readiness: { runtime_running: true, entries_enabled: true },
      approved_models: { rows: [{ lane_id: "lane-1" }] },
      alerts_state: {
        active_alerts: [{ id: "alert-1" }],
        recent_events: [{ id: "event-1" }],
        rows: [{ id: "row-1" }],
      },
    },
    historical_playback: {
      study_catalog: {
        items: [{
          study_key: "study-1",
          label: "Study 1",
          strategy_id: "strategy-1",
          symbol: "GC",
          study_mode: "baseline_parity_mode",
          coverage_start: "2026-04-01T00:00:00Z",
          coverage_end: "2026-04-17T00:00:00Z",
          closed_trade_count: 2,
          summary: {
            closed_trade_count: 2,
            calendar_breakdown: [{ date: "2026-04-17", realized_pnl: "-4156", trade_count: 18 }],
            closed_trade_breakdown: [{ exit_timestamp: "2026-04-17T12:00:00Z", realized_pnl: "-4156" }],
          },
          study_preview: { heavy: true },
        }],
      },
    },
    strategy_analysis: {
      results_board: {
        row_count: 25,
        rows: [{ strategy_key: "alpha" }],
      },
      details_by_strategy_key: {
        alpha: { note: "heavy" },
      },
      research_analytics: {
        available: true,
      },
    },
  }));
  __testing.setLoadLiveDashboardHook(async () => null);

  const state = await getDesktopState({ includeHeavyPayload: false });
  const dashboard = (state.dashboard ?? {}) as Record<string, unknown>;
  const paper = (dashboard.paper ?? {}) as Record<string, unknown>;
  const alertsState = (paper.alerts_state ?? {}) as Record<string, unknown>;
  const playback = (dashboard.historical_playback ?? {}) as Record<string, unknown>;
  const studyCatalog = (playback.study_catalog ?? {}) as Record<string, unknown>;
  const compactedItems = Array.isArray(studyCatalog.items) ? studyCatalog.items as Array<Record<string, unknown>> : [];
  const strategyAnalysis = (dashboard.strategy_analysis ?? {}) as Record<string, unknown>;
  const resultsBoard = (strategyAnalysis.results_board ?? {}) as Record<string, unknown>;

  assert.equal(state.connection, "snapshot");
  assert.equal(dashboard.desktop_compacted_for_startup, true);
  assert.deepEqual(alertsState.active_alerts, []);
  assert.deepEqual(alertsState.recent_events, []);
  assert.equal(compactedItems.length, 1);
  assert.deepEqual((compactedItems[0]?.summary as Record<string, unknown>)?.calendar_breakdown, [
    { date: "2026-04-17", realized_pnl: "-4156", trade_count: 18 },
  ]);
  assert.equal((compactedItems[0] as Record<string, unknown>)?.study_preview, undefined);
  assert.deepEqual(resultsBoard.rows, []);
  assert.deepEqual(strategyAnalysis.details_by_strategy_key, {});
  assert.deepEqual(strategyAnalysis.research_analytics, { available: true });
  __testing.resetRuntimeState();
});

test("startup with no live dashboard and no snapshots returns quickly while background bootstrap starts", async () => {
  __testing.resetRuntimeState();

  const previousSetting = process.env.MGC_DESKTOP_AUTO_BOOTSTRAP;
  process.env.MGC_DESKTOP_AUTO_BOOTSTRAP = "1";

  let bootstrapCalls = 0;
  let releaseBootstrap!: () => void;
  const bootstrapGate = new Promise<void>((resolve) => {
    releaseBootstrap = resolve;
  });

  __testing.setBuildLocalOperatorAuthStateHook(async () => makeDesktopState().localAuth);
  __testing.setLoadLiveDashboardHook(async () => null);
  __testing.setLoadSnapshotBundleHook(async () => null);
  __testing.setEnsureServiceHostUsableHook(async () => {
    bootstrapCalls += 1;
    await bootstrapGate;
  });

  const state = await Promise.race([
    getDesktopState(),
    new Promise<DesktopState>((_resolve, reject) => setTimeout(() => reject(new Error("getDesktopState timed out")), 1000)),
  ]);

  assert.ok(bootstrapCalls >= 1);
  assert.equal(state.source.mode, "degraded_reconnecting");
  assert.equal(state.backend.state, "starting");

  releaseBootstrap();
  await new Promise((resolve) => setTimeout(resolve, 0));
  if (previousSetting === undefined) {
    delete process.env.MGC_DESKTOP_AUTO_BOOTSTRAP;
  } else {
    process.env.MGC_DESKTOP_AUTO_BOOTSTRAP = previousSetting;
  }
  __testing.resetRuntimeState();
});

test("prepareDesktopForLaunch schedules service warmup without blocking the app", async () => {
  __testing.resetRuntimeState();

  const previousSetting = process.env.MGC_DESKTOP_AUTO_BOOTSTRAP;
  process.env.MGC_DESKTOP_AUTO_BOOTSTRAP = "1";

  let bootstrapCalls = 0;
  let releaseBootstrap!: () => void;
  const bootstrapGate = new Promise<void>((resolve) => {
    releaseBootstrap = resolve;
  });

  __testing.setLoadLiveDashboardHook(async () => null);
  __testing.setEnsureServiceHostUsableHook(async () => {
    bootstrapCalls += 1;
    await bootstrapGate;
  });

  await Promise.race([
    prepareDesktopForLaunch(),
    new Promise<void>((_resolve, reject) => setTimeout(() => reject(new Error("prepareDesktopForLaunch timed out")), 500)),
  ]);

  assert.equal(bootstrapCalls, 1);
  releaseBootstrap();
  await new Promise((resolve) => setTimeout(resolve, 0));
  if (previousSetting === undefined) {
    delete process.env.MGC_DESKTOP_AUTO_BOOTSTRAP;
  } else {
    process.env.MGC_DESKTOP_AUTO_BOOTSTRAP = previousSetting;
  }
  __testing.resetRuntimeState();
});

test("prepareDesktopForLaunch skips automatic warmup in sandboxed launch contexts", async () => {
  __testing.resetRuntimeState();

  const previousSetting = process.env.MGC_DESKTOP_AUTO_BOOTSTRAP;
  process.env.MGC_DESKTOP_AUTO_BOOTSTRAP = "0";

  let bootstrapCalls = 0;
  __testing.setLoadLiveDashboardHook(async () => null);
  __testing.setEnsureServiceHostUsableHook(async () => {
    bootstrapCalls += 1;
  });

  await Promise.race([
    prepareDesktopForLaunch(),
    new Promise<void>((_resolve, reject) => setTimeout(() => reject(new Error("prepareDesktopForLaunch timed out")), 500)),
  ]);

  assert.equal(bootstrapCalls, 0);

  if (previousSetting === undefined) {
    delete process.env.MGC_DESKTOP_AUTO_BOOTSTRAP;
  } else {
    process.env.MGC_DESKTOP_AUTO_BOOTSTRAP = previousSetting;
  }
  __testing.resetRuntimeState();
});

test("failed dashboard actions surface normalized blocker message and detail", async () => {
  __testing.resetRuntimeState();
  const liveState = makeDesktopState({
    connection: "live",
    backendUrl: "http://127.0.0.1:8790/",
    source: {
      mode: "live_api",
      label: "LIVE API",
      detail: "Using live /health and /api/dashboard responses from the local dashboard server.",
      canRunLiveActions: true,
      healthReachable: true,
      apiReachable: true,
    },
    backend: {
      state: "healthy",
      label: "HEALTHY",
      detail: "Backend health and dashboard API are both responding.",
      lastError: null,
      nextRetryAt: null,
      retryCount: 0,
      pid: 12345,
      apiStatus: "responding",
      healthStatus: "ok",
      managerOwned: true,
      startupFailureKind: "none",
      actionHint: null,
      staleListenerDetected: false,
      healthReachable: true,
      dashboardApiTimedOut: false,
      portConflictDetected: false,
    },
  });
  __testing.setGetDesktopStateHook(async () => liveState);
  __testing.setFetchHook(async () => new Response(
    JSON.stringify({
      ok: false,
      action_label: "Restart Runtime + Temp Paper",
      message: "Restart Paper Soak With Temp Paper blocked: AUTH_NOT_READY | Next action: Auth Gate Check",
      detail: "AUTH_NOT_READY | Next action: Auth Gate Check",
      output: "Paper runtime stopped; manual intervention required because broker/auth readiness is not green yet.",
      reason_code: "AUTH_NOT_READY",
      next_action: "Auth Gate Check",
    }),
    {
      status: 200,
      headers: { "Content-Type": "application/json" },
    },
  ));

  const result = await runDashboardAction("restart-paper-with-temp-paper");

  assert.equal(result.ok, false);
  assert.equal(result.message, "Restart Paper Soak With Temp Paper blocked: AUTH_NOT_READY | Next action: Auth Gate Check");
  assert.equal(result.detail, "AUTH_NOT_READY | Next action: Auth Gate Check");
  assert.equal(result.output, "Paper runtime stopped; manual intervention required because broker/auth readiness is not green yet.");
  __testing.resetRuntimeState();
});

test("auth gate check remains runnable from snapshot fallback without live API attachment", async () => {
  __testing.resetRuntimeState();
  const snapshotState = makeDesktopState({
    connection: "snapshot",
    source: {
      mode: "snapshot_fallback",
      label: "API NOT READY",
      detail: "Live /health is reachable, but /api/dashboard is not ready.",
      canRunLiveActions: false,
      healthReachable: true,
      apiReachable: false,
    },
    backend: {
      state: "degraded",
      label: "API NOT READY",
      detail: "Backend health is reachable, but the full /api/dashboard payload is not responsive.",
      lastError: "refresh_token_authentication_error",
      nextRetryAt: null,
      retryCount: 0,
      pid: null,
      apiStatus: "timed_out",
      healthStatus: "degraded",
      healthReachable: true,
      dashboardApiTimedOut: true,
      managerOwned: false,
      startupFailureKind: "dashboard_api_not_ready",
      actionHint: "Run Auth Gate Check.",
      staleListenerDetected: false,
      portConflictDetected: false,
    },
    backendUrl: "http://127.0.0.1:8790/",
  });
  __testing.setGetDesktopStateHook(async () => snapshotState);
  __testing.setExecScriptHook(async (args) => {
    assert.deepEqual(args, ["scripts/run_schwab_auth_gate.sh"]);
    return {
      ok: true,
      stdout: '{"runtime_ready": false, "message": "refresh failed"}',
      stderr: "",
      code: 0,
    };
  });

  const result = await runDashboardAction("auth-gate-check");

  assert.equal(result.ok, true);
  assert.equal(result.message, "Auth Gate Check completed.");
  assert.match(result.detail ?? "", /refresh failed/);

  __testing.resetRuntimeState();
});

test("health-only auth failure starts managed auth recovery automatically", async () => {
  __testing.resetRuntimeState();
  const previousSetting = process.env.MGC_DESKTOP_AUTO_BOOTSTRAP;
  process.env.MGC_DESKTOP_AUTO_BOOTSTRAP = "1";
  let recoveryCalls = 0;
  let resolveRecovery!: () => void;
  const recoveryGate = new Promise<void>((resolve) => {
    resolveRecovery = resolve;
  });
  __testing.setBuildLocalOperatorAuthStateHook(async () => ({
    auth_available: true,
    auth_platform: "macOS",
    auth_method: "TOUCH_ID",
    last_authenticated_at: "2026-04-09T07:23:03.873Z",
    last_auth_result: "SUCCEEDED",
    last_auth_detail: null,
    auth_session_expires_at: "2026-04-09T15:23:03.873Z",
    auth_session_ttl_seconds: 28800,
    auth_session_active: true,
    local_operator_identity: "local_touch_id_operator",
    auth_session_id: "session-123",
    touch_id_available: true,
    secret_protection: {
      available: true,
      provider: "KEYCHAIN_SAFE_STORAGE",
      wrapper_ready: true,
      wrapper_path: "/tmp/local_secret_wrapper.json",
      protects_token_file_directly: false,
      detail: "safeStorage ready",
    },
    latest_event: null,
    recent_events: [],
    artifacts: {
      state_path: "/tmp/local_operator_auth_state.json",
      events_path: "/tmp/local_operator_auth_events.jsonl",
      secret_wrapper_path: "/tmp/local_secret_wrapper.json",
    },
  }));
  __testing.setExecScriptHook(async (args) => {
    recoveryCalls += 1;
    assert.deepEqual(args, ["scripts/run_schwab_auth_gate.sh"]);
    await recoveryGate;
    return {
      ok: true,
      stdout: '{"runtime_ready": false, "message": "refresh still pending"}',
      stderr: "",
      code: 0,
    };
  });
  __testing.setLoadLiveDashboardHook(async () => ({
    mode: "health-only",
    url: "http://127.0.0.1:8790/",
    health: { status: "degraded", ready: false },
    error: "refresh_token_authentication_error",
  }));
  __testing.setLoadSnapshotBundleHook(async () => ({
    generated_at: new Date().toISOString(),
    operator_surface: { generated_at: new Date().toISOString(), runtime_readiness: { values: {} } },
  }));

  const state = await getDesktopState();

  assert.equal(state.source.mode, "degraded_reconnecting");
  assert.equal(state.backend.state, "reconnecting");
  assert.match(state.source.detail, /Automatic Schwab auth recovery is active/i);
  assert.ok(recoveryCalls >= 1);

  resolveRecovery();
  await new Promise((resolve) => setTimeout(resolve, 0));
  if (previousSetting === undefined) {
    delete process.env.MGC_DESKTOP_AUTO_BOOTSTRAP;
  } else {
    process.env.MGC_DESKTOP_AUTO_BOOTSTRAP = previousSetting;
  }
  __testing.resetRuntimeState();
});

test("live dashboard payload stays attached and actionable even when backend health is degraded", async () => {
  __testing.resetRuntimeState();
  __testing.setBuildLocalOperatorAuthStateHook(async () => ({
    auth_available: true,
    auth_platform: "macOS",
    auth_method: "TOUCH_ID",
    last_authenticated_at: "2026-04-09T07:23:03.873Z",
    last_auth_result: "SUCCEEDED",
    last_auth_detail: null,
    auth_session_expires_at: "2026-04-09T15:23:03.873Z",
    auth_session_ttl_seconds: 28800,
    auth_session_active: true,
    local_operator_identity: "local_touch_id_operator",
    auth_session_id: "session-123",
    touch_id_available: true,
    secret_protection: {
      available: true,
      provider: "KEYCHAIN_SAFE_STORAGE",
      wrapper_ready: true,
      wrapper_path: "/tmp/local_secret_wrapper.json",
      protects_token_file_directly: false,
      detail: "safeStorage ready",
    },
    latest_event: null,
    recent_events: [],
    artifacts: {
      state_path: "/tmp/local_operator_auth_state.json",
      events_path: "/tmp/local_operator_auth_events.jsonl",
      secret_wrapper_path: "/tmp/local_secret_wrapper.json",
    },
  }));
  __testing.setLoadLiveDashboardHook(async () => ({
    mode: "live",
    url: "http://127.0.0.1:8790/",
    health: { status: "degraded", ready: false, error: "refresh_token_authentication_error" },
    dashboard: {
      generated_at: new Date().toISOString(),
      dashboard_meta: { source: "artifact_snapshot_fallback", snapshot_fallback_active: true },
      global: { auth_ready: true },
      operator_surface: { generated_at: new Date().toISOString(), runtime_readiness: { values: {} } },
      startup_control_plane: {
        overall_state: "DEGRADED",
        launch_allowed: false,
        counts: { ready: 0, warming: 0, blocked: 0, degraded: 1, reconciliation_required: 0, needs_attention_now: 1 },
        primary_dependency: {
          key: "dashboard_backend",
          state: "DEGRADED",
          next_action_label: "Refresh",
          next_action_detail: "Refresh after auth recovery.",
          next_action_kind: "refresh",
        },
      },
      supervised_paper_operability: {
        state: "ATTENTION_REQUIRED",
        app_usable_for_supervised_paper: false,
        unusable_reason: "Schwab auth must be refreshed before the paper runtime can be trusted.",
        summary_line: "Schwab auth must be refreshed before the paper runtime can be trusted.",
        primary_next_action: "Auth Gate Check",
      },
      paper: {
        readiness: { runtime_running: false, runtime_phase: "STOPPED", entries_enabled: true },
        running: false,
      },
      production_link: {},
    },
  }));

  const state = await getDesktopState();

  assert.equal(state.connection, "live");
  assert.equal(state.source.mode, "live_api");
  assert.equal(state.source.label, "SERVICE ATTACHED");
  assert.equal(state.source.canRunLiveActions, true);
  assert.equal(state.backend.apiStatus, "responding");
  assert.equal(state.backend.healthStatus, "degraded");
  __testing.resetRuntimeState();
});

test("production-link actions retry after a transient local transport failure", async () => {
  __testing.resetRuntimeState();
  __testing.setAutoBootstrapBlockedHook(() => false);
  const liveState = makeDesktopState({
    connection: "live",
    backendUrl: "http://127.0.0.1:8790/",
    source: {
      mode: "live_api",
      label: "LIVE API",
      detail: "Using live /health and /api/dashboard responses from the local dashboard server.",
      canRunLiveActions: true,
      healthReachable: true,
      apiReachable: true,
    },
    backend: {
      state: "healthy",
      label: "HEALTHY",
      detail: "Backend health and dashboard API are both responding.",
      lastError: null,
      nextRetryAt: null,
      retryCount: 0,
      pid: 12345,
      apiStatus: "responding",
      healthStatus: "ok",
      managerOwned: true,
      startupFailureKind: "none",
      actionHint: null,
      staleListenerDetected: false,
      healthReachable: true,
      dashboardApiTimedOut: false,
      portConflictDetected: false,
    },
  });
  __testing.setGetDesktopStateHook(async () => liveState);
  __testing.setEnsureServiceHostUsableHook(async () => {});
  let attempts = 0;
  __testing.setFetchHook(async (input, init) => {
    attempts += 1;
    assert.equal(String(input), "http://127.0.0.1:8790/api/production-link/preview-order");
    assert.equal(init?.method, "POST");
    if (attempts === 1) {
      throw new Error("fetch failed");
    }
    return new Response(
      JSON.stringify({
        ok: true,
        action_label: "Preview Broker Order",
        message: "Built a dry-run broker payload preview without sending a live order.",
      }),
      {
        status: 200,
        headers: { "Content-Type": "application/json" },
      },
    );
  });

  try {
    const result = await runProductionLinkAction("preview-order", {
      symbol: "MGC",
      asset_class: "FUTURE",
      intent_type: "MANUAL_LIVE_FUTURES_PILOT",
    });

    assert.equal(result.ok, true);
    assert.equal(result.message, "Built a dry-run broker payload preview without sending a live order.");
    assert.equal(result.detail, undefined);
    assert.equal(attempts, 2);
  } finally {
    __testing.resetRuntimeState();
  }
});

test("production-link actions surface retried localhost transport detail when the local API stays unavailable", async () => {
  __testing.resetRuntimeState();
  const liveState = makeDesktopState({
    connection: "live",
    backendUrl: "http://127.0.0.1:8790/",
    source: {
      mode: "live_api",
      label: "LIVE API",
      detail: "Using live /health and /api/dashboard responses from the local dashboard server.",
      canRunLiveActions: true,
      healthReachable: true,
      apiReachable: true,
    },
    backend: {
      state: "healthy",
      label: "HEALTHY",
      detail: "Backend health and dashboard API are both responding.",
      lastError: null,
      nextRetryAt: null,
      retryCount: 0,
      pid: 12345,
      apiStatus: "responding",
      healthStatus: "ok",
      managerOwned: true,
      startupFailureKind: "none",
      actionHint: null,
      staleListenerDetected: false,
      healthReachable: true,
      dashboardApiTimedOut: false,
      portConflictDetected: false,
    },
  });
  __testing.setGetDesktopStateHook(async () => liveState);
  __testing.setEnsureServiceHostUsableHook(async () => {});
  __testing.setFetchHook(async () => {
    throw new Error("fetch failed");
  });

  const result = await runProductionLinkAction("preview-order", {
    symbol: "MGC",
    asset_class: "FUTURE",
    intent_type: "MANUAL_LIVE_FUTURES_PILOT",
  });

  assert.equal(result.ok, false);
  assert.equal(result.message, "Failed to run production-link action preview-order.");
  assert.match(result.detail ?? "", /Local production-link API transport failed\./);
  assert.match(result.detail ?? "", /http:\/\/127\.0\.0\.1:8790\/api\/production-link\/preview-order/);
  __testing.resetRuntimeState();
});

test("sandboxed production-link transport failure does not trigger backend bootstrap retry", async () => {
  __testing.resetRuntimeState();
  __testing.setAutoBootstrapBlockedHook(() => true);
  const liveState = makeDesktopState({
    connection: "live",
    backendUrl: "http://127.0.0.1:8790/",
    source: {
      mode: "live_api",
      label: "LIVE API",
      detail: "Using live /health and /api/dashboard responses from the local dashboard server.",
      canRunLiveActions: true,
      healthReachable: true,
      apiReachable: true,
    },
    backend: {
      state: "healthy",
      label: "HEALTHY",
      detail: "Backend health and dashboard API are both responding.",
      lastError: null,
      nextRetryAt: null,
      retryCount: 0,
      pid: 12345,
      apiStatus: "responding",
      healthStatus: "ok",
      managerOwned: true,
      startupFailureKind: "none",
      actionHint: null,
      staleListenerDetected: false,
      healthReachable: true,
      dashboardApiTimedOut: false,
      portConflictDetected: false,
    },
  });
  let ensureCalls = 0;
  let fetchCalls = 0;
  __testing.setGetDesktopStateHook(async () => liveState);
  __testing.setEnsureServiceHostUsableHook(async () => {
    ensureCalls += 1;
  });
  __testing.setFetchHook(async () => {
    fetchCalls += 1;
    throw new Error("fetch failed");
  });

  try {
    const result = await runProductionLinkAction("preview-order", { symbol: "MGC" });
    assert.equal(result.ok, false);
    assert.match(result.detail ?? "", /Local production-link API transport failed\./);
    assert.equal(fetchCalls, 1);
    assert.equal(ensureCalls, 0);
  } finally {
    __testing.resetRuntimeState();
  }
});

test("production-link success prefers backend message and output detail", async () => {
  __testing.resetRuntimeState();
  const liveState = makeDesktopState({
    connection: "live",
    backendUrl: "http://127.0.0.1:8790/",
    source: {
      mode: "live_api",
      label: "LIVE API",
      detail: "Using live /health and /api/dashboard responses from the local dashboard server.",
      canRunLiveActions: true,
      healthReachable: true,
      apiReachable: true,
    },
    backend: {
      state: "healthy",
      label: "HEALTHY",
      detail: "Backend health and dashboard API are both responding.",
      lastError: null,
      nextRetryAt: null,
      retryCount: 0,
      pid: 12345,
      apiStatus: "responding",
      healthStatus: "ok",
      managerOwned: true,
      startupFailureKind: "none",
      actionHint: null,
      staleListenerDetected: false,
      healthReachable: true,
      dashboardApiTimedOut: false,
      portConflictDetected: false,
    },
  });
  __testing.setGetDesktopStateHook(async () => liveState);
  __testing.setFetchHook(async () =>
    new Response(
      JSON.stringify({
        ok: true,
        action: "preview-order",
        action_label: "Send Manual Broker Order",
        message: "Submitted manual broker order for MGC.",
        output: '{"broker_order_id":"abc123"}',
      }),
      {
        status: 200,
        headers: { "Content-Type": "application/json" },
      },
    ),
  );

  const result = await runProductionLinkAction("preview-order", { symbol: "MGC" });

  assert.equal(result.ok, true);
  assert.equal(result.message, "Submitted manual broker order for MGC.");
  assert.equal(result.detail, '{"broker_order_id":"abc123"}');
  __testing.resetRuntimeState();
});

test("production-link empty 200 response is surfaced as failure", async () => {
  __testing.resetRuntimeState();
  const liveState = makeDesktopState({
    connection: "live",
    backendUrl: "http://127.0.0.1:8790/",
    source: {
      mode: "live_api",
      label: "LIVE API",
      detail: "Using live /health and /api/dashboard responses from the local dashboard server.",
      canRunLiveActions: true,
      healthReachable: true,
      apiReachable: true,
    },
    backend: {
      state: "healthy",
      label: "HEALTHY",
      detail: "Backend health and dashboard API are both responding.",
      lastError: null,
      nextRetryAt: null,
      retryCount: 0,
      pid: 12345,
      apiStatus: "responding",
      healthStatus: "ok",
      managerOwned: true,
      startupFailureKind: "none",
      actionHint: null,
      staleListenerDetected: false,
      healthReachable: true,
      dashboardApiTimedOut: false,
      portConflictDetected: false,
    },
  });
  __testing.setGetDesktopStateHook(async () => liveState);
  __testing.setFetchHook(async () => new Response("", { status: 200 }));

  const result = await runProductionLinkAction("preview-order", { symbol: "MGC" });

  assert.equal(result.ok, false);
  assert.match(result.message, /empty response/i);
  assert.match(result.detail ?? "", /without a JSON body/i);
  __testing.resetRuntimeState();
});
