import test from "node:test";
import assert from "node:assert/strict";

import { __testing, runDashboardAction, startDashboard, type DesktopState } from "./runtime";

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
    },
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
