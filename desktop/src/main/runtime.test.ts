import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

import {
  __testing,
  compactDesktopStateForRenderer,
  DESKTOP_RENDERER_TRANSFER_BUDGET_BYTES,
  getDesktopState,
  prepareDesktopForLaunch,
  runDashboardAction,
  runProductionLinkAction,
  startDashboard,
  type DesktopState,
} from "./runtime";
import { buildOperatorTriageContract } from "./shared/operatorTriage";

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
    trackB: {
      operatorStatusPath: "/tmp/latest_operator_status_summary.json",
      available: false,
      malformed: false,
      status: null,
      missingReason: "No Track B operator status artifact found.",
      loadedAt: new Date().toISOString(),
      ...(overrides.trackB ?? {}),
    },
    refreshedAt: new Date().toISOString(),
    ...overrides,
  };
}

function makeOversizedDashboardFixture(): Record<string, unknown> {
  const heavyTradeLog = Array.from({ length: 500 }, (_value, index) => {
    const exitTime = new Date(Date.UTC(2026, 3, 29, 15, 0, 0) - index * 60 * 60 * 1000).toISOString();
    const entryTime = new Date(Date.parse(exitTime) - 20 * 60 * 1000).toISOString();
    return {
      trade_id: `trade-${index}`,
      lane_id: `lane-${index % 8}`,
      instrument: index % 2 === 0 ? "MGC" : "MNQ",
      status: "CLOSED",
      entry_timestamp: entryTime,
      exit_timestamp: exitTime,
      realized_pnl: index % 2 === 0 ? 12.5 : -7.25,
      note: "X".repeat(8_000),
    };
  });
  const heavyAlertMap = Object.fromEntries(
    Array.from({ length: 3_500 }, (_value, index) => [
      `alert-${index}`,
      {
        severity: "warn",
        detail: "Y".repeat(1_400),
      },
    ]),
  );
  const heavyStrategyDetails = Object.fromEntries(
    Array.from({ length: 160 }, (_value, index) => [
      `strategy-${index}`,
      {
        note: "Z".repeat(30_000),
      },
    ]),
  );
  const heavyResultsRows = Array.from({ length: 2_000 }, (_value, index) => ({
    strategy_key: `strategy-${index}`,
    summary: "R".repeat(2_000),
  }));
  const heavyUnifiedDetailViews = {
    giant: {
      rows: Array.from({ length: 1_500 }, (_value, index) => ({
        strategy_key: `strategy-${index}`,
        detail: "U".repeat(2_000),
      })),
    },
  };
  return {
    generated_at: "2026-04-29T21:59:00Z",
    dashboard_meta: {
      server_instance_id: "desktop-budget-fixture",
    },
    operator_surface: {
      generated_at: "2026-04-29T21:59:00Z",
      runtime_readiness: {
        values: {
          blocking_fault_count: 0,
          advisory_fault_count: 1,
        },
      },
    },
    paper: {
      readiness: {
        paper_trade_allowed: true,
        paper_trade_block_reason: null,
        live_trade_allowed: false,
        live_trade_block_reason: "LIVE_AUTHORITY_REQUIRED",
        paper_runtime_ready: true,
        paper_readiness_source: "src/mgc_v05l/app/operator_dashboard.py:_paper_readiness_payload",
        session_eligible_count: 8,
        waiting_for_bar_count: 4,
        no_setup_count: 11,
        actionable_now_count: 1,
        true_blocked_count: 0,
        blocking_fault_count: 0,
        advisory_fault_count: 1,
        lane_eligibility_rows: [
          {
            lane_id: "gc_1x_all_lanes__asia_early_long",
            route_ready: true,
            route_destination: "ibkr_paper_bridge_submit_capable",
            bridge_allowed: true,
            bridge_block_reason: null,
          },
        ],
      },
      alerts_state: {
        active_alerts: [{ id: "active-1" }],
        recent_events: [{ id: "recent-1" }],
        rows: [{ id: "row-1" }],
        by_key: heavyAlertMap,
      },
      strategy_performance: {
        generated_at: "2026-04-29T21:59:00Z",
        trade_log: heavyTradeLog,
        rows: [{ lane_id: "gc_1x_all_lanes__asia_early_long" }],
      },
      raw_operator_status: {
        lanes: Array.from({ length: 44 }, (_value, index) => ({
          lane_id: `lane-${index}`,
          explanation: "O".repeat(8_000),
        })),
        active_lane_ids: ["gc_1x_all_lanes__asia_early_long"],
      },
      signal_intent_fill_audit: {
        rows: Array.from({ length: 44 }, (_value, index) => ({
          lane_id: `lane-${index}`,
          explanation: "A".repeat(8_000),
        })),
        summary: { row_count: 44 },
        artifacts: {
          report_path: "/tmp/paper_signal_audit.md",
        },
      },
      events: {
        alerts: Array.from({ length: 10 }, (_value, index) => ({ id: index, detail: "E".repeat(2_000) })),
      },
    },
    strategy_analysis: {
      generated_at: "2026-04-29T21:59:00Z",
      results_board: {
        row_count: heavyResultsRows.length,
        rows: heavyResultsRows,
      },
      details_by_strategy_key: heavyStrategyDetails,
      unified_monitor: {
        available: true,
        detail_views: heavyUnifiedDetailViews,
        selection_summary: {
          selected_strategy_key: "gc_family",
        },
      },
      research_analytics: {
        available: true,
      },
    },
    historical_playback: {
      study_catalog: {
        items: Array.from({ length: 300 }, (_value, index) => ({
          study_key: `study-${index}`,
          label: `Study ${index}`,
          summary: {
            calendar_breakdown: [{ date: "2026-04-29", realized_pnl: "0", trade_count: 1 }],
          },
        })),
      },
    },
    production_link: {
      diagnostics: {
        open_orders_total: 0,
      },
      reconciliation: {
        broker_minus_ledger: {
          MGC: 0,
          MNQ: 0,
          MES: 0,
        },
      },
      broker_state_snapshot: {
        positions: {
          MGC: 0,
          MNQ: 0,
          MES: 0,
        },
      },
      portfolio: {
        ledger_positions: {
          MGC: 0,
          MNQ: 0,
          MES: 0,
        },
      },
    },
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

test("electron renderer readiness cards map corrected fireability fields", () => {
  const appTsx = fs.readFileSync(path.resolve(__dirname, "../../src/renderer/App.tsx"), "utf8");

  assert.match(appTsx, /label:\s*"Session Eligible"[\s\S]*session_eligible_lanes_count/);
  assert.match(appTsx, /label:\s*"Waiting For 3m Bar"[\s\S]*waiting_for_completed_bar_count/);
  assert.match(appTsx, /label:\s*"Evaluated \/ No Setup"[\s\S]*no_setup_count/);
  assert.match(appTsx, /label:\s*"Actionable Now"[\s\S]*actionable_now_count/);
  assert.match(appTsx, /label:\s*"Blocked Lanes"[\s\S]*blocked_lanes_count/);
  assert.match(appTsx, /label:\s*"Ready This Bar"[\s\S]*eligible_to_trade_count/);
  assert.doesNotMatch(appTsx, /title:\s*"Tradable Now"/);
});

test("Track B read-only status loads latest operator status artifact without invoking runtime actions", async () => {
  const previous = process.env.MGC_TRACK_B_OPERATOR_STATUS_SUMMARY_PATH;
  const tempPath = path.join("/private/tmp", `tmp_track_b_operator_status_test_${process.pid}.json`);
  fs.writeFileSync(
    tempPath,
    JSON.stringify(
      {
        status_verdict: "OPERATOR_STATUS_OK_FOR_SHADOW_REVIEW",
        required_next_action: "Review no-submit artifacts.",
        observation_runner_verdict: "TRACK_B_OBSERVATION_RUNNER_COMPLETED_FOR_REVIEW",
        observation_runner_mode: "watch",
        observation_runner_current_cycle: 2,
        observation_runner_watch_exited_normally: true,
        observation_runner_required_next_action: "Review Track B Status UI.",
        observation_runner_databento_observer_verdict: "DATABENTO_CANDLE_OBSERVER_WROTE_EVENT",
        observation_runner_strategy_adapter_verdict: "STRATEGY_SIGNAL_ADAPTER_EMITTED_SIGNAL_BATCH",
        observation_runner_signal_batch_writer_verdict: "SIGNAL_BATCH_WRITER_WROTE_BATCH",
        observation_runner_listener_verdict: "SHADOW_LISTENER_CYCLE_COMPLETED",
        observation_runner_submit_allowed: false,
        observation_runner_submit_attempted: false,
        observation_runner_live_money_readiness: false,
        databento_observer_verdict: "DATABENTO_CANDLE_OBSERVER_WROTE_EVENT",
        databento_observer_mode: "watch",
        databento_observer_current_cycle: 2,
        databento_observer_last_verdict: "DATABENTO_CANDLE_OBSERVER_WROTE_EVENT",
        strategy_adapter_verdict: "STRATEGY_SIGNAL_ADAPTER_EMITTED_SIGNAL_BATCH",
        candle_producer_verdict: "CANDLE_SIGNAL_PRODUCER_PRODUCED_SIGNAL_BATCH",
        signal_batch_writer_verdict: "SIGNAL_BATCH_WRITER_WROTE_BATCH",
        submit_allowed: false,
        submit_attempted: false,
        live_money_readiness: false,
      },
      null,
      2,
    ),
    "utf8",
  );
  process.env.MGC_TRACK_B_OPERATOR_STATUS_SUMMARY_PATH = tempPath;
  try {
    const trackB = await __testing.buildTrackBReadOnlyStatus();
    assert.equal(trackB.available, true);
    assert.equal(trackB.malformed, false);
    assert.equal(trackB.operatorStatusPath, tempPath);
    assert.equal(trackB.status?.status_verdict, "OPERATOR_STATUS_OK_FOR_SHADOW_REVIEW");
    assert.equal(trackB.status?.observation_runner_verdict, "TRACK_B_OBSERVATION_RUNNER_COMPLETED_FOR_REVIEW");
    assert.equal(trackB.status?.observation_runner_mode, "watch");
    assert.equal(trackB.status?.databento_observer_verdict, "DATABENTO_CANDLE_OBSERVER_WROTE_EVENT");
    assert.equal(trackB.status?.databento_observer_mode, "watch");
    assert.equal(trackB.status?.strategy_adapter_verdict, "STRATEGY_SIGNAL_ADAPTER_EMITTED_SIGNAL_BATCH");
    assert.equal(trackB.status?.candle_producer_verdict, "CANDLE_SIGNAL_PRODUCER_PRODUCED_SIGNAL_BATCH");
    assert.equal(trackB.status?.submit_allowed, false);
  } finally {
    if (previous === undefined) {
      delete process.env.MGC_TRACK_B_OPERATOR_STATUS_SUMMARY_PATH;
    } else {
      process.env.MGC_TRACK_B_OPERATOR_STATUS_SUMMARY_PATH = previous;
    }
    fs.rmSync(tempPath, { force: true });
  }
});

test("Track B read-only status handles missing and malformed artifacts safely", async () => {
  const previous = process.env.MGC_TRACK_B_OPERATOR_STATUS_SUMMARY_PATH;
  const missingPath = path.join("/private/tmp", `missing_track_b_operator_status_${process.pid}.json`);
  process.env.MGC_TRACK_B_OPERATOR_STATUS_SUMMARY_PATH = missingPath;
  try {
    const missing = await __testing.buildTrackBReadOnlyStatus();
    assert.equal(missing.available, false);
    assert.equal(missing.malformed, false);
    assert.match(missing.missingReason ?? "", /No Track B operator status artifact found/);

    fs.writeFileSync(missingPath, "{not-json", "utf8");
    const malformed = await __testing.buildTrackBReadOnlyStatus();
    assert.equal(malformed.available, false);
    assert.equal(malformed.malformed, true);
    assert.match(malformed.missingReason ?? "", /Could not read Track B operator status artifact/);
  } finally {
    if (previous === undefined) {
      delete process.env.MGC_TRACK_B_OPERATOR_STATUS_SUMMARY_PATH;
    } else {
      process.env.MGC_TRACK_B_OPERATOR_STATUS_SUMMARY_PATH = previous;
    }
    fs.rmSync(missingPath, { force: true });
  }
});

test("Track B status renderer is display-only and no-submit", () => {
  const appTsx = fs.readFileSync(path.resolve(__dirname, "../../src/renderer/App.tsx"), "utf8");

  assert.match(appTsx, /TrackBStatusPage/);
  assert.match(appTsx, /NO-SUBMIT \/ SHADOW REVIEW/);
  assert.match(appTsx, /Observation Runner/);
  assert.match(appTsx, /observation_runner_verdict/);
  assert.match(appTsx, /observation_runner_latest_operator_status_path/);
  assert.match(appTsx, /Market Data Observer/);
  assert.match(appTsx, /databento_observer_verdict/);
  assert.match(appTsx, /databento_observer_last_verdict/);
  assert.match(appTsx, /databento_output_event_path/);
  assert.match(appTsx, /Upstream Signal Chain/);
  assert.match(appTsx, /strategy_adapter_verdict/);
  assert.match(appTsx, /candle_producer_verdict/);
  assert.match(appTsx, /signal_batch_writer_verdict/);
  assert.match(appTsx, /latest_operator_status_summary\.json/);
  assert.match(appTsx, /page !== "track-b" && !PRIMARY_WORKSTATION_PAGES\.has\(page\)/);
  assert.match(appTsx, /const showSidebarEmergencyHalt = page !== "track-b"/);
  assert.doesNotMatch(appTsx, /page === "track-b"[\s\S]{0,2000}runDashboardAction/);
  assert.doesNotMatch(appTsx, /page === "track-b"[\s\S]{0,2000}paper_proof_cli/);
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
    transportKind: "readiness_bridge",
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

test("packaged snapshot authority rejects stale desktop cache when fresher readiness-backed artifacts exist", () => {
  const freshArtifacts = {
    generated_at: "2026-04-30T07:01:13.562Z",
    operator_surface: {
      generated_at: "2026-04-30T07:01:13.562Z",
    },
    paper: {
      running: true,
      readiness: {
        generated_at: "2026-04-30T07:01:13.562Z",
        current_broad_trading_session: "LONDON_EARLY",
        lane_eligibility_rows: [],
        runtime_running: true,
        paper_runtime_ready: true,
      },
    },
    global: {
      mode: "PAPER",
      mode_label: "PAPER",
    },
    dashboard_meta: {
      source: "artifact_snapshot",
    },
  };
  const staleDesktopCache = {
    generated_at: "2026-04-18T07:01:13.562Z",
    operator_surface: {
      generated_at: "2026-04-18T07:01:13.562Z",
    },
    paper: {
      running: true,
      readiness: {
        generated_at: "2026-04-18T07:01:13.562Z",
        current_broad_trading_session: "UNCLASSIFIED",
        lane_eligibility_rows: [],
        runtime_running: true,
        paper_runtime_ready: true,
      },
    },
    global: {
      mode: "PAPER",
      mode_label: "PAPER",
    },
    dashboard_meta: {
      source: "desktop_cache",
    },
  };

  const selected = __testing.selectPackagedSnapshotCandidate([
    { name: "desktop_cache", snapshot: staleDesktopCache },
    { name: "fresh_operator_artifacts", snapshot: freshArtifacts },
  ]);

  assert.equal(selected?.name, "fresh_operator_artifacts");
});

test("packaged snapshot authority returns no candidate instead of silently promoting stale cache as current paper state", () => {
  const staleDesktopCache = {
    generated_at: "2026-04-18T07:01:13.562Z",
    operator_surface: {
      generated_at: "2026-04-18T07:01:13.562Z",
    },
    paper: {
      running: true,
      readiness: {
        generated_at: "2026-04-18T07:01:13.562Z",
        current_broad_trading_session: "UNCLASSIFIED",
        lane_eligibility_rows: [],
        runtime_running: true,
        paper_runtime_ready: true,
      },
    },
    global: {
      mode: "PAPER",
      mode_label: "PAPER",
    },
    dashboard_meta: {
      source: "desktop_cache",
    },
  };

  const selected = __testing.selectPackagedSnapshotCandidate([
    { name: "desktop_cache", snapshot: staleDesktopCache },
  ]);

  assert.equal(selected, null);
});

test("attached readiness authority prefers fresher workspace readiness over stale local cache when session truth diverges", () => {
  const snapshot = {
    generated_at: "2026-04-30T07:01:13.562Z",
    operator_surface: {
      generated_at: "2026-04-30T07:01:13.562Z",
    },
    paper: {
      running: true,
      readiness: {
        generated_at: "2026-04-30T07:01:13.562Z",
        current_broad_trading_session: "LONDON_EARLY",
        lane_eligibility_rows: [],
        runtime_running: true,
        paper_runtime_ready: true,
      },
    },
    global: {
      mode: "PAPER",
      mode_label: "PAPER",
    },
    dashboard_meta: {
      server_instance_id: "instance-current",
      source: "artifact_snapshot",
    },
  };
  const staleLocalReadiness = {
    generated_at: "2026-04-30T06:58:00.000Z",
    current_broad_trading_session: "UNCLASSIFIED",
    readiness_state: "READY",
    payload: {
      reachable: true,
      ready: true,
      instance_id: "instance-current",
    },
    listener: {
      reachable: true,
    },
    control_plane: {
      dashboard_attached: true,
      launch_allowed: true,
      paper_runtime_ready: true,
    },
  };
  const freshWorkspaceReadiness = {
    generated_at: "2026-04-30T07:01:13.562Z",
    current_broad_trading_session: "LONDON_EARLY",
    readiness_state: "READY",
    payload: {
      reachable: true,
      ready: true,
      instance_id: "instance-current",
    },
    listener: {
      reachable: true,
    },
    control_plane: {
      dashboard_attached: true,
      launch_allowed: true,
      paper_runtime_ready: true,
    },
  };

  const selected = __testing.selectAttachedReadinessCandidate([
    { name: "desktop_local_readiness", readiness: staleLocalReadiness },
    { name: "workspace_readiness", readiness: freshWorkspaceReadiness },
  ], snapshot);

  assert.equal(selected?.name, "workspace_readiness");
});

test("attached readiness authority rejects stale wrong-session cache when fresher snapshot proves the current session", () => {
  const snapshot = {
    generated_at: "2026-04-30T07:01:13.562Z",
    operator_surface: {
      generated_at: "2026-04-30T07:01:13.562Z",
    },
    paper: {
      running: true,
      readiness: {
        generated_at: "2026-04-30T07:01:13.562Z",
        current_broad_trading_session: "LONDON_EARLY",
        lane_eligibility_rows: [],
        runtime_running: true,
        paper_runtime_ready: true,
      },
    },
    global: {
      mode: "PAPER",
      mode_label: "PAPER",
    },
    dashboard_meta: {
      server_instance_id: "instance-current",
      source: "artifact_snapshot",
    },
  };
  const staleLocalReadiness = {
    generated_at: "2026-04-30T06:58:00.000Z",
    current_broad_trading_session: "UNCLASSIFIED",
    readiness_state: "READY",
    payload: {
      reachable: true,
      ready: true,
      instance_id: "instance-current",
    },
    listener: {
      reachable: true,
    },
    control_plane: {
      dashboard_attached: true,
      launch_allowed: true,
      paper_runtime_ready: true,
    },
  };

  const selected = __testing.selectAttachedReadinessCandidate([
    { name: "desktop_local_readiness", readiness: staleLocalReadiness },
  ], snapshot);

  assert.equal(selected, null);
});

test("packaged launch promotes to live API when the local dashboard endpoint is reachable", async () => {
  __testing.resetRuntimeState();
  __testing.setPackagedLocalBundleLaunchContextHook(() => true);
  __testing.setBuildLocalOperatorAuthStateHook(async () => makeDesktopState().localAuth);
  __testing.setLoadSnapshotBundleHook(async () => ({
    generated_at: new Date().toISOString(),
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
  __testing.setLoadLiveDashboardHook(async () => ({
    mode: "live",
    url: "http://127.0.0.1:8790/",
    health: { status: "ok", ready: true, pid: 98165 },
    dashboard: {
      generated_at: new Date().toISOString(),
      dashboard_meta: {
        source: "service_live_api",
        server_instance_id: "instance-current",
        server_pid: 98165,
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
      },
    },
  }));

  const state = await getDesktopState();

  assert.equal(state.connection, "live");
  assert.equal(state.source.mode, "live_api");
  assert.equal(state.source.apiReachable, true);
  assert.equal(state.source.canRunLiveActions, true);
  assert.equal(state.backend.apiStatus, "responding");
  assert.equal(state.backend.pid, 98165);
  __testing.resetRuntimeState();
});

test("packaged launch does not stay in attached snapshot bridge once a ready live API responds after the snapshot grace window", async () => {
  __testing.resetRuntimeState();
  __testing.setPackagedLocalBundleLaunchContextHook(() => true);
  __testing.setBuildLocalOperatorAuthStateHook(async () => makeDesktopState().localAuth);
  __testing.setLoadSnapshotBundleHook(async () => ({
    generated_at: new Date().toISOString(),
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
  __testing.setLoadLiveDashboardHook(
    async () =>
      await new Promise((resolve) =>
        setTimeout(
          () =>
            resolve({
              mode: "live",
              url: "http://127.0.0.1:8790/",
              health: { status: "ok", ready: true, pid: 98165 },
              dashboard: {
                generated_at: new Date().toISOString(),
                dashboard_meta: {
                  source: "service_live_api",
                  server_instance_id: "instance-current",
                  server_pid: 98165,
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
                },
              },
            }),
          2000,
        ),
      ),
  );

  const startedAt = Date.now();
  const state = await getDesktopState();

  assert.equal(state.connection, "live");
  assert.equal(state.source.mode, "live_api");
  assert.equal(state.source.apiReachable, true);
  assert.equal(state.backend.apiStatus, "responding");
  assert.ok(Date.now() - startedAt >= 1900);
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

test("health-only backend prefers attached degraded bridge over snapshot fallback messaging", async () => {
  __testing.resetRuntimeState();

  __testing.setLoadLiveDashboardHook(async () => ({
    mode: "health-only",
    url: "http://127.0.0.1:8790/",
    health: { status: "degraded", ready: false },
    error: "dashboard payload timeout",
  }));
  __testing.setBuildLocalOperatorAuthStateHook(async () => makeDesktopState().localAuth);
  __testing.setLoadSnapshotBundleHook(async () => ({
    generated_at: new Date().toISOString(),
    dashboard_meta: { server_instance_id: "instance-current" },
    global: { mode: "IDLE", mode_label: "IDLE", auth_ready: true, runtime_status: "STOPPED" },
    operator_surface: { generated_at: new Date().toISOString(), runtime_readiness: { values: {} } },
    paper: { readiness: { runtime_running: false, entries_enabled: true }, running: false },
    startup_control_plane: {},
    production_link: {},
  }));
  __testing.setLoadAttachedSnapshotBridgeHook(async () => ({
    transportKind: "readiness_bridge",
    readiness: { readiness_state: "NOT_READY", control_plane: { launch_allowed: false, dashboard_attached: true } },
    health: { status: "degraded", ready: false },
    backendUrl: "http://127.0.0.1:8790/",
    detail: "Service is attached and current, but supervised paper remains blocked and requires operator attention.",
  }));

  const state = await getDesktopState();

  assert.equal(state.connection, "snapshot");
  assert.equal(state.source.mode, "attached_snapshot_bridge");
  assert.equal(state.source.label, "SERVICE ATTACHED / DEGRADED");
  assert.equal(state.backend.state, "degraded");
  assert.deepEqual(state.errors, []);

  __testing.resetRuntimeState();
});

test("packaged launch keeps live API attachment when the readiness bridge confirms the current payload even if a direct probe misses", async () => {
  __testing.resetRuntimeState();
  __testing.setPackagedLocalBundleLaunchContextHook(() => true);
  __testing.setBuildLocalOperatorAuthStateHook(async () => makeDesktopState().localAuth);
  __testing.setLoadSnapshotBundleHook(async () => ({
    generated_at: new Date().toISOString(),
    dashboard_meta: {
      server_instance_id: "instance-current",
      server_pid: 98165,
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
    },
  }));
  __testing.setLoadAttachedSnapshotBridgeHook(async () => ({
    transportKind: "readiness_bridge",
    readiness: {
      readiness_state: "READY",
      payload: {
        reachable: true,
        ready: true,
        instance_id: "instance-current",
        pid: 98165,
      },
      listener: {
        reachable: true,
      },
      control_plane: {
        launch_allowed: true,
        dashboard_attached: true,
        paper_runtime_ready: true,
      },
      configured_url: "http://127.0.0.1:8790/",
    },
    health: { status: "ok", ready: true, pid: 98165 },
    backendUrl: "http://127.0.0.1:8790/",
    detail: "Service is attached through the local readiness bridge and synchronized operator snapshot.",
  }));
  __testing.setLoadLiveDashboardHook(async () => null);

  const state = await getDesktopState();

  assert.equal(state.connection, "live");
  assert.equal(state.source.mode, "live_api");
  assert.equal(state.source.apiReachable, true);
  assert.equal(state.source.canRunLiveActions, true);
  assert.equal(state.backend.apiStatus, "responding");
  __testing.resetRuntimeState();
});

test("paper mode does not let live broker and operator auth gates block supervised paper usability", () => {
  const contract = buildOperatorTriageContract({
    desktopSourceMode: "attached_snapshot_bridge",
    desktopRefreshedAt: new Date().toISOString(),
    dashboardGeneratedAt: new Date().toISOString(),
    global: {
      mode: "PAPER",
      mode_label: "PAPER",
      live_disabled: true,
      market_data_status: "LIVE",
      market_data_label: "LIVE",
      reconciliation_status: "CLEAN",
      stale: false,
    },
    operatorSurface: {
      generated_at: new Date().toISOString(),
    },
    runtimeReadiness: {
      runtime_status: "RUNNING",
      paper_enabled: true,
      entries_enabled: true,
      blocking_faults_active: false,
    },
    runtimeValues: {
      runtime_recovery_state: "RUNNING",
    },
    paperReadiness: {
      generated_at: new Date().toISOString(),
      runtime_running: true,
      entries_enabled: true,
    },
    portfolio: {},
    productionLinkEnabled: true,
    productionLink: {
      operator_status: {
        local_operator_auth: {
          available: true,
          ready: false,
          auth_session_active: false,
          entry_allowed: false,
          flatten_allowed: true,
          replace_allowed: false,
          blocker: "Local operator auth session expired.",
        },
      },
      futures_pilot_status: {
        preview_blockers: ["Futures pilot preview is disabled because MGC_PRODUCTION_FUTURES_PILOT_ENABLED is false."],
        live_submit_blockers: ["Futures pilot live submit remains preview-only until FUTURE:MARKET is explicitly live-verified."],
      },
    },
    productionHealth: {
      broker_reachable: { ok: true, detail: "reachable" },
      auth_healthy: { ok: true, detail: "healthy" },
      account_selected: { ok: true, detail: "selected" },
      positions_fresh: { ok: true, detail: "fresh" },
      quotes_fresh: { ok: true, detail: "fresh" },
    },
    productionReconciliation: {
      blocked: false,
      mismatch_count: 0,
      detail: "clear",
    },
    productionDiagnostics: {},
    productionBalances: {},
    localOperatorAuth: {
      auth_session_active: false,
      last_auth_detail: "expired",
    },
    operatorActiveAlertRows: [],
    operatorRecentAlertRows: [],
    sameUnderlyingConflictSummary: {},
  });

  assert.equal(contract.operator_triage.paper_trade_authority, "Enabled");
  assert.equal(contract.operator_triage.paper_trade_allowed, true);
  assert.equal(contract.operator_triage.paper_trade_block_reason, null);
  assert.equal(contract.operator_triage.live_trade_authority, "Blocked");
  assert.equal(contract.operator_triage.paper_bridge_allowed, true);
  assert.equal(contract.operator_triage.live_bridge_allowed, false);
  assert.equal(contract.operator_triage.paper_readiness_source, null);
  assert.equal(contract.operator_triage.verdict_sentence, "Paper stack healthy. Flat. Paper trade authority enabled.");
  assert.equal(contract.operator_triage.root_cause.code, "no_hard_gate_failure");
  assert.equal(contract.operator_triage.hard_gates.find((gate) => gate.key === "broker-authority")?.status, "pass");
  assert.equal(contract.operator_triage.hard_gates.find((gate) => gate.key === "operator-authority")?.status, "pass");
});

test("paper mode keeps live authority blocked while allowing supervised paper authority", () => {
  const contract = buildOperatorTriageContract({
    desktopSourceMode: "attached_snapshot_bridge",
    desktopRefreshedAt: new Date().toISOString(),
    dashboardGeneratedAt: new Date().toISOString(),
    global: {
      mode: "PAPER",
      mode_label: "PAPER",
      live_disabled: true,
      market_data_status: "LIVE",
      market_data_label: "LIVE",
      reconciliation_status: "CLEAN",
      stale: false,
    },
    operatorSurface: {
      generated_at: new Date().toISOString(),
    },
    runtimeReadiness: {
      runtime_status: "RUNNING",
      paper_enabled: true,
      entries_enabled: true,
      blocking_faults_active: false,
    },
    runtimeValues: {
      runtime_recovery_state: "RUNNING",
    },
    paperReadiness: {
      generated_at: new Date().toISOString(),
      runtime_running: true,
      entries_enabled: true,
    },
    portfolio: {},
    productionLinkEnabled: true,
    productionLink: {
      operator_status: {
        local_operator_auth: {
          available: true,
          ready: false,
          auth_session_active: false,
          entry_allowed: false,
          flatten_allowed: true,
          replace_allowed: false,
          blocker: "Local operator auth session expired.",
        },
      },
      futures_pilot_status: {
        preview_blockers: ["Futures pilot preview is disabled because MGC_PRODUCTION_FUTURES_PILOT_ENABLED is false."],
        live_submit_blockers: ["Futures pilot live submit remains preview-only until FUTURE:MARKET is explicitly live-verified."],
      },
    },
    productionHealth: {
      broker_reachable: { ok: true, detail: "reachable" },
      auth_healthy: { ok: true, detail: "healthy" },
      account_selected: { ok: true, detail: "selected" },
      positions_fresh: { ok: true, detail: "fresh" },
      quotes_fresh: { ok: true, detail: "fresh" },
    },
    productionReconciliation: {
      blocked: false,
      mismatch_count: 0,
      detail: "clear",
    },
    productionDiagnostics: {},
    productionBalances: {},
    localOperatorAuth: {
      auth_session_active: false,
      last_auth_detail: "expired",
    },
    operatorActiveAlertRows: [],
    operatorRecentAlertRows: [],
    sameUnderlyingConflictSummary: {},
  });

  assert.equal(contract.operator_triage.paper_trade_authority, "Enabled");
  assert.equal(contract.operator_triage.paper_trade_allowed, true);
  assert.equal(contract.operator_triage.live_trade_authority, "Blocked");
  assert.equal(contract.operator_triage.paper_bridge_allowed, true);
  assert.equal(contract.operator_triage.live_bridge_allowed, false);
});

test("paper mode still hard-blocks on current runtime faults with paper-specific wording", () => {
  const contract = buildOperatorTriageContract({
    desktopSourceMode: "attached_snapshot_bridge",
    desktopRefreshedAt: new Date().toISOString(),
    dashboardGeneratedAt: new Date().toISOString(),
    global: {
      mode: "PAPER",
      mode_label: "PAPER",
      live_disabled: true,
      market_data_status: "LIVE",
      market_data_label: "LIVE",
      reconciliation_status: "CLEAN",
      stale: false,
      runtime_health_label: "FAULTED",
    },
    operatorSurface: {
      generated_at: new Date().toISOString(),
    },
    runtimeReadiness: {
      runtime_status: "RUNNING",
      paper_enabled: true,
      entries_enabled: true,
      blocking_faults_active: true,
      status_line: "runtime=RUNNING | faults=1 | advisory=0",
    },
    runtimeValues: {
      runtime_recovery_state: "RUNNING",
    },
    paperReadiness: {
      generated_at: new Date().toISOString(),
      runtime_running: true,
      entries_enabled: true,
    },
    portfolio: {},
    productionLinkEnabled: true,
    productionLink: {},
    productionHealth: {
      broker_reachable: { ok: true, detail: "reachable" },
      auth_healthy: { ok: true, detail: "healthy" },
      account_selected: { ok: true, detail: "selected" },
      positions_fresh: { ok: true, detail: "fresh" },
      quotes_fresh: { ok: true, detail: "fresh" },
    },
    productionReconciliation: {
      blocked: false,
      mismatch_count: 0,
      detail: "clear",
    },
    productionDiagnostics: {},
    productionBalances: {},
    localOperatorAuth: {},
    operatorActiveAlertRows: [],
    operatorRecentAlertRows: [],
    sameUnderlyingConflictSummary: {},
  });

  assert.equal(contract.operator_triage.paper_trade_authority, "Blocked");
  assert.equal(contract.operator_triage.paper_trade_allowed, false);
  assert.equal(contract.operator_triage.paper_trade_block_reason, "Paper trade authority is unavailable.");
  assert.equal(contract.operator_triage.dominant_blocker.code, "paper_trade_authority_blocked");
  assert.equal(contract.operator_triage.verdict_sentence, "Flat but blocked. Paper trade authority is not currently available.");
});

test("live mode still requires live trade authority", () => {
  const contract = buildOperatorTriageContract({
    desktopSourceMode: "live_api",
    desktopRefreshedAt: new Date().toISOString(),
    dashboardGeneratedAt: new Date().toISOString(),
    global: {
      mode: "LIVE",
      mode_label: "LIVE",
      live_disabled: false,
      market_data_status: "LIVE",
      market_data_label: "LIVE",
      reconciliation_status: "CLEAN",
      stale: false,
    },
    operatorSurface: {
      generated_at: new Date().toISOString(),
    },
    runtimeReadiness: {
      runtime_status: "RUNNING",
      paper_enabled: true,
      entries_enabled: true,
      blocking_faults_active: false,
    },
    runtimeValues: {
      runtime_recovery_state: "RUNNING",
    },
    paperReadiness: {
      generated_at: new Date().toISOString(),
      runtime_running: true,
      entries_enabled: true,
    },
    portfolio: {},
    productionLinkEnabled: true,
    productionLink: {
      operator_status: {
        local_operator_auth: {
          available: true,
          ready: false,
          auth_session_active: false,
          entry_allowed: false,
          flatten_allowed: true,
          replace_allowed: false,
          blocker: "Local operator auth session expired.",
        },
      },
      futures_pilot_status: {
        preview_blockers: [],
        live_submit_blockers: [],
      },
    },
    productionHealth: {
      broker_reachable: { ok: true, detail: "reachable" },
      auth_healthy: { ok: true, detail: "healthy" },
      account_selected: { ok: true, detail: "selected" },
      positions_fresh: { ok: true, detail: "fresh" },
      quotes_fresh: { ok: true, detail: "fresh" },
    },
    productionReconciliation: {
      blocked: false,
      mismatch_count: 0,
      detail: "clear",
    },
    productionDiagnostics: {},
    productionBalances: {},
    localOperatorAuth: {
      auth_session_active: false,
      last_auth_detail: "expired",
    },
    operatorActiveAlertRows: [],
    operatorRecentAlertRows: [],
    sameUnderlyingConflictSummary: {},
  });

  assert.equal(contract.operator_triage.live_trade_authority, "Blocked");
  assert.equal(contract.operator_triage.live_trade_allowed, false);
  assert.equal(contract.operator_triage.dominant_blocker.code, "live_trade_authority_blocked");
  assert.equal(contract.operator_triage.verdict_sentence, "Flat but blocked. Live trade authority is not currently available.");
});

test("paper mode follows authoritative paper readiness contract instead of recomputing from live-style gate mixes", () => {
  const contract = buildOperatorTriageContract({
    desktopSourceMode: "attached_snapshot_bridge",
    desktopRefreshedAt: new Date().toISOString(),
    dashboardGeneratedAt: new Date().toISOString(),
    global: {
      mode: "PAPER",
      mode_label: "PAPER",
      live_disabled: true,
      market_data_status: "STALE",
      market_data_label: "STALE",
      reconciliation_status: "CLEAN",
      stale: true,
      runtime_health_label: "FAULTED",
    },
    operatorSurface: {
      generated_at: new Date().toISOString(),
    },
    runtimeReadiness: {
      runtime_status: "RUNNING",
      paper_enabled: true,
      entries_enabled: true,
      blocking_faults_active: false,
      status_line: "runtime=RUNNING | paper=ENABLED",
    },
    runtimeValues: {
      runtime_recovery_state: "RUNNING",
      paper_trade_allowed: true,
      paper_trade_block_reason: null,
      paper_readiness_source: "src/mgc_v05l/app/operator_dashboard.py:_paper_readiness_payload",
      paper_readiness_timestamp: new Date().toISOString(),
      session_eligible_count: 9,
      waiting_for_bar_count: 9,
      no_setup_count: 11,
      actionable_now_count: 0,
      true_blocked_count: 0,
      advisory_fault_count: 9,
      blocking_fault_count: 0,
    },
    paperReadiness: {
      generated_at: new Date().toISOString(),
      runtime_running: true,
      entries_enabled: true,
      paper_runtime_ready: true,
      paper_trade_allowed: true,
      paper_trade_block_reason: null,
    },
    portfolio: {},
    productionLinkEnabled: true,
    productionLink: {
      operator_status: {
        local_operator_auth: {
          available: true,
          ready: false,
          auth_session_active: false,
          entry_allowed: false,
          flatten_allowed: true,
          replace_allowed: false,
          blocker: "Local operator auth session expired.",
        },
      },
      futures_pilot_status: {
        preview_blockers: ["preview blocked"],
        live_submit_blockers: ["live blocked"],
      },
    },
    productionHealth: {
      broker_reachable: { ok: false, detail: "down" },
      auth_healthy: { ok: false, detail: "down" },
      account_selected: { ok: false, detail: "down" },
      positions_fresh: { ok: false, detail: "down" },
      quotes_fresh: { ok: false, detail: "down" },
    },
    productionReconciliation: {
      blocked: false,
      mismatch_count: 0,
      detail: "clear",
    },
    productionDiagnostics: {},
    productionBalances: {},
    localOperatorAuth: {
      auth_session_active: false,
      last_auth_detail: "expired",
    },
    operatorActiveAlertRows: [],
    operatorRecentAlertRows: [],
    sameUnderlyingConflictSummary: {},
  });

  assert.equal(contract.operator_triage.paper_trade_authority, "Enabled");
  assert.equal(contract.operator_triage.paper_trade_allowed, true);
  assert.equal(contract.operator_triage.live_trade_authority, "Blocked");
  assert.equal(contract.operator_triage.paper_readiness_source, "src/mgc_v05l/app/operator_dashboard.py:_paper_readiness_payload");
  assert.equal(contract.operator_triage.session_eligible_count, 9);
  assert.equal(contract.operator_triage.waiting_for_bar_count, 9);
  assert.equal(contract.operator_triage.advisory_fault_count, 9);
  assert.equal(contract.operator_triage.blocking_fault_count, 0);
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
  const state = compactDesktopStateForRenderer(makeDesktopState({
    dashboard: {
    generated_at: new Date().toISOString(),
    operator_surface: { generated_at: new Date().toISOString(), runtime_readiness: { values: {} } },
    paper: {
      readiness: { runtime_running: true, entries_enabled: true },
      approved_models: { rows: [{ lane_id: "lane-1" }] },
      alerts_state: {
        active_alerts: [{ id: "alert-1" }],
        recent_events: [{ id: "event-1" }],
        rows: [{ id: "row-1" }],
        by_key: { "alert-1": { severity: "warn" } },
      },
      strategy_performance: {
        generated_at: "2026-04-18T09:00:00Z",
        trade_log: [{ trade_id: "t-1" }, { trade_id: "t-2" }],
        rows: [{ lane_id: "lane-1" }],
      },
      raw_operator_status: {
        lanes: [{ lane_id: "lane-1" }],
        active_lane_ids: ["lane-1"],
      },
      signal_intent_fill_audit: {
        rows: [{ lane_id: "lane-1", audit_verdict: "SETUP_GATED" }],
        summary: { row_count: 1 },
      },
      events: {
        alerts: [{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }, { id: 5 }, { id: 6 }],
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
      unified_monitor: {
        detail_views: { giant: { rows: [{ strategy_key: "alpha" }] } },
        selection_summary: { selected_strategy_key: "alpha" },
      },
      research_analytics: {
        available: true,
      },
    },
  },
  }));
  const dashboard = (state.dashboard ?? {}) as Record<string, unknown>;
  const paper = (dashboard.paper ?? {}) as Record<string, unknown>;
  const alertsState = (paper.alerts_state ?? {}) as Record<string, unknown>;
  const playback = (dashboard.historical_playback ?? {}) as Record<string, unknown>;
  const studyCatalog = (playback.study_catalog ?? {}) as Record<string, unknown>;
  const compactedItems = Array.isArray(studyCatalog.items) ? studyCatalog.items as Array<Record<string, unknown>> : [];
  const strategyAnalysis = (dashboard.strategy_analysis ?? {}) as Record<string, unknown>;
  const resultsBoard = (strategyAnalysis.results_board ?? {}) as Record<string, unknown>;
  const unifiedMonitor = (strategyAnalysis.unified_monitor ?? {}) as Record<string, unknown>;
  const strategyPerformance = (paper.strategy_performance ?? {}) as Record<string, unknown>;
  const rawOperatorStatus = (paper.raw_operator_status ?? {}) as Record<string, unknown>;
  const signalIntentFillAudit = (paper.signal_intent_fill_audit ?? {}) as Record<string, unknown>;
  const events = (paper.events ?? {}) as Record<string, unknown>;
  const transferMeta = ((dashboard.dashboard_meta ?? {}) as Record<string, unknown>).desktop_transfer as Record<string, unknown>;

  assert.equal(state.connection, "snapshot");
  assert.equal(dashboard.desktop_compacted_for_startup, true);
  assert.deepEqual(alertsState.active_alerts, [{ id: "alert-1" }]);
  assert.deepEqual(alertsState.recent_events, [{ id: "event-1" }]);
  assert.deepEqual(alertsState.by_key, {});
  assert.equal(alertsState.alert_count, 1);
  assert.equal(compactedItems.length, 1);
  assert.deepEqual((compactedItems[0]?.summary as Record<string, unknown>)?.calendar_breakdown, [
    { date: "2026-04-17", realized_pnl: "-4156", trade_count: 18 },
  ]);
  assert.equal((compactedItems[0] as Record<string, unknown>)?.study_preview, undefined);
  assert.deepEqual(resultsBoard.rows, []);
  assert.deepEqual(strategyAnalysis.details_by_strategy_key, {});
  assert.equal((unifiedMonitor.detail_views as Record<string, unknown> | undefined), undefined);
  assert.equal(unifiedMonitor.compacted_for_startup, true);
  assert.deepEqual(strategyAnalysis.research_analytics, { available: true });
  assert.equal(Array.isArray(strategyPerformance.trade_log), true);
  assert.equal((strategyPerformance.trade_log as Array<unknown>).length, 2);
  assert.equal(strategyPerformance.trade_log_count, 2);
  assert.deepEqual(rawOperatorStatus.lanes, [{ lane_id: "lane-1" }]);
  assert.equal(rawOperatorStatus.lane_count, 1);
  assert.deepEqual(signalIntentFillAudit.rows, [{ lane_id: "lane-1", audit_verdict: "SETUP_GATED" }]);
  assert.equal(signalIntentFillAudit.row_count, 1);
  assert.equal(Array.isArray(events.alerts), true);
  assert.equal((events.alerts as Array<unknown>).length, 5);
  assert.equal(events.alerts_count, 6);
  assert.equal(transferMeta.compacted_for_startup, true);
  assert.equal(transferMeta.budget_bytes, DESKTOP_RENDERER_TRANSFER_BUDGET_BYTES);
  assert.equal(typeof (transferMeta.detail_artifacts as Record<string, unknown>).full_dashboard_snapshot_path, "string");
});

test("renderer-bound desktop state stays under budget while preserving operator-critical fields", () => {
  const dashboard = makeOversizedDashboardFixture();
  const rawBytes = Buffer.byteLength(JSON.stringify(dashboard));
  assert.ok(rawBytes > DESKTOP_RENDERER_TRANSFER_BUDGET_BYTES);

  const baseState = makeDesktopState();
  const state = makeDesktopState({
    connection: "live",
    backendUrl: "http://127.0.0.1:8790/",
    source: {
      mode: "live_api",
      label: "Live API",
      detail: "attached",
      canRunLiveActions: true,
      healthReachable: true,
      apiReachable: true,
    },
    backend: {
      state: "healthy",
      label: "Healthy",
      detail: "attached",
      lastError: null,
      nextRetryAt: null,
      retryCount: 0,
      pid: 123,
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
      ...baseState.startup,
      mode: "SERVICE_ATTACHED",
      ownership: "attached_existing",
      chosenHost: "127.0.0.1",
      chosenPort: 8790,
      chosenUrl: "http://127.0.0.1:8790/",
      failureKind: "none",
      recommendedAction: null,
      staleListenerDetected: false,
      healthReachable: true,
      dashboardApiTimedOut: false,
    },
    dashboard,
  });

  const compactedState = compactDesktopStateForRenderer(state);
  const compactedDashboard = (compactedState.dashboard ?? {}) as Record<string, unknown>;
  const compactedBytes = Buffer.byteLength(JSON.stringify(compactedDashboard));
  const paper = (compactedDashboard.paper ?? {}) as Record<string, unknown>;
  const readiness = (paper.readiness ?? {}) as Record<string, unknown>;
  const strategyAnalysis = (compactedDashboard.strategy_analysis ?? {}) as Record<string, unknown>;
  const resultsBoard = (strategyAnalysis.results_board ?? {}) as Record<string, unknown>;
  const unifiedMonitor = (strategyAnalysis.unified_monitor ?? {}) as Record<string, unknown>;
  const alertsState = (paper.alerts_state ?? {}) as Record<string, unknown>;
  const strategyPerformance = (paper.strategy_performance ?? {}) as Record<string, unknown>;
  const tradeLogWindow = (strategyPerformance.trade_log_window ?? {}) as Record<string, unknown>;
  const rawOperatorStatus = (paper.raw_operator_status ?? {}) as Record<string, unknown>;
  const signalIntentFillAudit = (paper.signal_intent_fill_audit ?? {}) as Record<string, unknown>;
  const productionLink = (compactedDashboard.production_link ?? {}) as Record<string, unknown>;
  const diagnostics = (productionLink.diagnostics ?? {}) as Record<string, unknown>;
  const reconciliation = (productionLink.reconciliation ?? {}) as Record<string, unknown>;
  const brokerStateSnapshot = (productionLink.broker_state_snapshot ?? {}) as Record<string, unknown>;
  const portfolio = (productionLink.portfolio ?? {}) as Record<string, unknown>;
  const transferMeta = ((compactedDashboard.dashboard_meta ?? {}) as Record<string, unknown>).desktop_transfer as Record<string, unknown>;

  assert.ok(compactedBytes < DESKTOP_RENDERER_TRANSFER_BUDGET_BYTES);
  assert.equal(compactedState.source.mode, "live_api");
  assert.equal(readiness.paper_trade_allowed, true);
  assert.equal(readiness.paper_trade_block_reason, null);
  assert.equal(readiness.paper_runtime_ready, true);
  assert.equal(readiness.actionable_now_count, 1);
  assert.equal(readiness.true_blocked_count, 0);
  assert.equal(readiness.blocking_fault_count, 0);
  assert.equal(readiness.session_eligible_count, 8);
  assert.equal(readiness.waiting_for_bar_count, 4);
  assert.equal(readiness.no_setup_count, 11);
  assert.equal((readiness.lane_eligibility_rows as Array<Record<string, unknown>>)[0]?.route_destination, "ibkr_paper_bridge_submit_capable");
  assert.equal((readiness.lane_eligibility_rows as Array<Record<string, unknown>>)[0]?.bridge_allowed, true);
  assert.equal(diagnostics.open_orders_total, 0);
  assert.deepEqual((brokerStateSnapshot.positions ?? {}) as Record<string, unknown>, { MGC: 0, MNQ: 0, MES: 0 });
  assert.deepEqual((portfolio.ledger_positions ?? {}) as Record<string, unknown>, { MGC: 0, MNQ: 0, MES: 0 });
  assert.deepEqual((reconciliation.broker_minus_ledger ?? {}) as Record<string, unknown>, { MGC: 0, MNQ: 0, MES: 0 });
  assert.deepEqual(resultsBoard.rows, []);
  assert.deepEqual(strategyAnalysis.details_by_strategy_key, {});
  assert.equal((unifiedMonitor.detail_views as Record<string, unknown> | undefined), undefined);
  assert.deepEqual(alertsState.by_key, {});
  assert.equal(Array.isArray(alertsState.active_alerts), true);
  assert.equal(Array.isArray(alertsState.recent_events), true);
  const compactTradeLog = strategyPerformance.trade_log as Array<Record<string, unknown>>;
  assert.equal(compactTradeLog.length, 200);
  assert.equal(strategyPerformance.trade_log_count, 500);
  assert.deepEqual(tradeLogWindow.requested_range, null);
  assert.equal(tradeLogWindow.total_trade_count, 500);
  assert.equal(tradeLogWindow.returned_trade_count, 200);
  assert.equal(tradeLogWindow.latest_trade_count, 200);
  assert.equal(String(compactTradeLog[0]?.exit_timestamp ?? ""), "2026-04-29T15:00:00.000Z");
  assert.equal(String(compactTradeLog[199]?.exit_timestamp ?? ""), "2026-04-21T08:00:00.000Z");
  assert.equal((rawOperatorStatus.lanes as Array<unknown>).length, 44);
  assert.equal((signalIntentFillAudit.rows as Array<unknown>).length, 44);
  assert.equal(transferMeta.compacted_for_startup, true);
  assert.equal(transferMeta.budget_bytes, DESKTOP_RENDERER_TRANSFER_BUDGET_BYTES);
  assert.equal(typeof (transferMeta.detail_artifacts as Record<string, unknown>).full_dashboard_snapshot_path, "string");
});

test("renderer paper trade-log contract unions the visible calendar window with latest live rows", () => {
  const dashboard = makeOversizedDashboardFixture();
  const state = makeDesktopState({
    connection: "live",
    source: {
      mode: "live_api",
      label: "Live API",
      detail: "attached",
      canRunLiveActions: true,
      healthReachable: true,
      apiReachable: true,
    },
    backend: {
      state: "healthy",
      label: "Healthy",
      detail: "attached",
      lastError: null,
      nextRetryAt: null,
      retryCount: 0,
      pid: 123,
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
    dashboard,
  });

  const compactedState = compactDesktopStateForRenderer(state, {
    paperTradeLogVisibleRange: {
      startDate: "2026-04-16",
      endDate: "2026-04-20",
    },
  });
  const compactedDashboard = (compactedState.dashboard ?? {}) as Record<string, unknown>;
  const paper = (compactedDashboard.paper ?? {}) as Record<string, unknown>;
  const strategyPerformance = (paper.strategy_performance ?? {}) as Record<string, unknown>;
  const compactTradeLog = strategyPerformance.trade_log as Array<Record<string, unknown>>;
  const tradeLogWindow = (strategyPerformance.trade_log_window ?? {}) as Record<string, unknown>;
  const returnedDays = [...new Set(compactTradeLog.map((row) => String(row.exit_timestamp ?? row.entry_timestamp ?? "").slice(0, 10)).filter(Boolean))].sort();

  assert.deepEqual(tradeLogWindow.requested_range, {
    startDate: "2026-04-16",
    endDate: "2026-04-20",
  });
  assert.equal(tradeLogWindow.total_trade_count, 500);
  assert.equal(tradeLogWindow.visible_range_complete, true);
  assert.ok(returnedDays.includes("2026-04-16"));
  assert.ok(returnedDays.includes("2026-04-17"));
  assert.ok(returnedDays.includes("2026-04-18"));
  assert.ok(returnedDays.includes("2026-04-19"));
  assert.ok(returnedDays.includes("2026-04-20"));
  assert.ok(returnedDays.includes("2026-04-29"));
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
