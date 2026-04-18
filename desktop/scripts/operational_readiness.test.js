const test = require("node:test");
const assert = require("node:assert/strict");

const {
  deriveOperationalReadiness,
} = require("../dist/main/shared/operationalReadiness.js");

function baseScenario() {
  return {
    connection: "live",
    backendUrl: "http://127.0.0.1:8790/",
    source: {
      mode: "live_api",
      label: "LIVE API",
      detail: "Using live dashboard responses.",
      canRunLiveActions: true,
      healthReachable: true,
      apiReachable: true,
    },
    backend: {
      state: "healthy",
      label: "HEALTHY",
      detail: "Backend health and dashboard API are both responding.",
      apiStatus: "responding",
      healthStatus: "ok",
      dashboardApiTimedOut: false,
    },
    startup: {
      mode: "SERVICE_ATTACHED",
      ownership: "attached_existing",
      failureKind: "none",
    },
    startupControlPlane: {
      overall_state: "READY",
      launch_allowed: true,
      summary_line: "Startup dependencies aligned.",
      primary_issue_title: "Ready",
      primary_reason: "Ready",
      counts: {
        ready: 5,
        warming: 0,
        blocked: 0,
        degraded: 0,
        reconciliation_required: 0,
        needs_attention_now: 0,
      },
      dependencies: [],
      primary_dependency: {
        label: "Dashboard backend",
        state: "READY",
        next_action_label: "Refresh",
        next_action_kind: "refresh",
        next_action_detail: "Refresh the startup dependency status.",
      },
    },
    supervisedPaperOperability: {
      app_usable_for_supervised_paper: true,
      state: "USABLE",
      summary_line: "Application is usable for supervised paper operation.",
      dashboard_attached: true,
      startup_ready: true,
      launch_allowed: true,
      runtime_running: true,
      paper_runtime_phase: "RUNNING",
      paper_runtime_ready: true,
      entries_enabled: true,
      operator_halt: false,
      operator_action_required: false,
      primary_next_action: "Refresh",
    },
    paperReadiness: {
      runtime_running: true,
      runtime_phase: "RUNNING",
      entries_enabled: true,
      operator_halt: false,
      summary_line: "Paper runtime is active.",
    },
    temporaryPaperRuntimeIntegrity: {
      mismatch_status: "MATCHED",
      temp_paper_blocked: false,
      summary_line: "Temp-paper runtime integrity is matched.",
    },
    authReadyForPaperStartup: true,
  };
}

test("A: live health + api + attached runtime yields READY without contradiction", () => {
  const model = deriveOperationalReadiness(baseScenario());

  assert.equal(model.overallState, "READY");
  assert.equal(model.appUsableForSupervisedPaper, true);
  assert.equal(model.dashboardAttached, true);
  assert.equal(model.liveActionsAllowed, true);
  assert.equal(model.paperRuntimeReady, true);
  assert.equal(model.launchAllowed, true);
  assert.notEqual(model.primaryIssueTitle, "Dashboard/API is not fully attached.");
});

test("A2: service-backed attach stays attached even if desktop-owned live-action hint is stale", () => {
  const scenario = baseScenario();
  scenario.source.canRunLiveActions = false;

  const model = deriveOperationalReadiness(scenario);

  assert.equal(model.overallState, "READY");
  assert.equal(model.dashboardAttached, true);
  assert.equal(model.liveActionsAllowed, true);
  assert.notEqual(model.primaryIssueTitle, "Dashboard/API is not fully attached.");
});

test("A3: attached snapshot bridge is operational but does not unlock live actions", () => {
  const scenario = baseScenario();
  scenario.connection = "snapshot";
  scenario.source = {
    mode: "attached_snapshot_bridge",
    label: "SERVICE ATTACHED",
    detail: "Using attached backend readiness with the latest persisted operator snapshot.",
    canRunLiveActions: false,
    healthReachable: true,
    apiReachable: false,
  };
  scenario.backend = {
    state: "healthy",
    label: "HEALTHY",
    detail: "Dashboard readiness is healthy, but direct localhost API transport is restricted in this launch context.",
    apiStatus: "unreachable",
    healthStatus: "unreachable",
    dashboardApiTimedOut: false,
  };
  scenario.startup = {
    mode: "SERVICE_ATTACHED",
    ownership: "attached_existing",
    failureKind: "none",
  };

  const model = deriveOperationalReadiness(scenario);

  assert.equal(model.overallState, "READY");
  assert.equal(model.dashboardAttached, true);
  assert.equal(model.appUsableForSupervisedPaper, true);
  assert.equal(model.liveActionsAllowed, false);
  assert.notEqual(model.primaryIssueTitle, "Dashboard/API attach is incomplete.");
  assert.notEqual(model.primaryIssueTitle, "Snapshot fallback is active.");
});

test("B: health-only state stays attach-incomplete and non-ready", () => {
  const scenario = baseScenario();
  scenario.source.canRunLiveActions = false;
  scenario.source.apiReachable = false;
  scenario.backend.apiStatus = "timed_out";
  scenario.backend.dashboardApiTimedOut = true;
  const model = deriveOperationalReadiness(scenario);

  assert.equal(model.overallState, "RECONCILING");
  assert.equal(model.dashboardAttached, false);
  assert.equal(model.liveActionsAllowed, false);
  assert.equal(model.launchAllowed, false);
  assert.match(model.primaryReason, /api\/dashboard/i);
});

test("C: snapshot fallback only stays degraded and non-green", () => {
  const scenario = baseScenario();
  scenario.connection = "snapshot";
  scenario.backendUrl = null;
  scenario.source = {
    mode: "snapshot_fallback",
    label: "SNAPSHOT FALLBACK",
    detail: "Using persisted operator snapshots because no live dashboard API is currently available.",
    canRunLiveActions: false,
    healthReachable: false,
    apiReachable: false,
  };
  scenario.backend = {
    state: "backend_down",
    label: "BACKEND DOWN",
    detail: "No live backend answered; the app is running from the latest persisted dashboard artifacts.",
    apiStatus: "unreachable",
    healthStatus: "unreachable",
    dashboardApiTimedOut: false,
  };

  const model = deriveOperationalReadiness(scenario);
  assert.equal(model.overallState, "RECONCILING");
  assert.equal(model.launchAllowed, false);
  assert.equal(model.dashboardAttached, false);
  assert.match(model.primaryIssueTitle, /snapshot fallback/i);
});

test("D: attached but paper halted or temp-paper blocked cannot present as paper-ready", () => {
  const halted = baseScenario();
  halted.paperReadiness.runtime_running = false;
  halted.paperReadiness.runtime_phase = "HALTED";
  halted.supervisedPaperOperability = {
    app_usable_for_supervised_paper: false,
    state: "PAPER_RUNTIME_HALTED",
    unusable_reason_code: "paper_runtime_not_running",
    unusable_reason: "Paper runtime is not currently running.",
    primary_next_action: "Start Runtime",
  };
  let model = deriveOperationalReadiness(halted);
  assert.equal(model.overallState, "ATTENTION_REQUIRED");
  assert.equal(model.appUsableForSupervisedPaper, false);
  assert.equal(model.paperRuntimeReady, false);
  assert.equal(model.paperRuntimeState, "HALTED");
  assert.equal(model.launchAllowed, false);

  const blocked = baseScenario();
  blocked.temporaryPaperRuntimeIntegrity.mismatch_status = "MISMATCH";
  blocked.temporaryPaperRuntimeIntegrity.temp_paper_blocked = true;
  blocked.temporaryPaperRuntimeIntegrity.block_reason_code = "enabled_lane_missing_from_runtime";
  blocked.temporaryPaperRuntimeIntegrity.block_reason = "Enabled temporary paper lanes are not loaded in the running paper runtime.";
  blocked.temporaryPaperRuntimeIntegrity.summary_line = "Temp-paper lanes are not loaded in runtime.";
  blocked.supervisedPaperOperability = {
    app_usable_for_supervised_paper: false,
    state: "PAPER_RUNTIME_BLOCKED",
    unusable_reason_code: "temp_paper_runtime_mismatch",
    unusable_reason: "Temp-paper runtime integrity is not yet matched.",
    primary_next_action: "Restart Runtime + Temp Paper",
  };
  model = deriveOperationalReadiness(blocked);
  assert.equal(model.overallState, "ATTENTION_REQUIRED");
  assert.equal(model.tempPaperBlocked, true);
  assert.equal(model.paperRuntimeReady, false);
  assert.equal(model.launchAllowed, false);
});

test("D2: CLEAR temp-paper integrity does not block a healthy attached runtime", () => {
  const scenario = baseScenario();
  scenario.temporaryPaperRuntimeIntegrity = {
    mismatch_status: "CLEAR",
    temp_paper_blocked: false,
    block_reason_code: null,
    block_reason: null,
    summary_line: "Enabled in app: 0 | loaded in runtime: 0 | snapshot only: 0 | missing lane ids: none",
    clearing_action: "Refresh",
  };

  const model = deriveOperationalReadiness(scenario);

  assert.equal(model.overallState, "READY");
  assert.equal(model.tempPaperBlocked, false);
  assert.equal(model.appUsableForSupervisedPaper, true);
});

test("F: running runtime with entries halted is not usable and points to Resume Entries", () => {
  const scenario = baseScenario();
  scenario.supervisedPaperOperability = {
    app_usable_for_supervised_paper: false,
    state: "PAPER_RUNTIME_HALTED",
    unusable_reason_code: "paper_entries_halted",
    unusable_reason: "Paper runtime is attached, but entries remain halted.",
    primary_next_action: "Resume Entries",
  };
  scenario.paperReadiness.runtime_running = true;
  scenario.paperReadiness.runtime_phase = "RUNNING";
  scenario.paperReadiness.entries_enabled = false;
  scenario.paperReadiness.operator_halt = true;

  const model = deriveOperationalReadiness(scenario);
  assert.equal(model.appUsableForSupervisedPaper, false);
  assert.equal(model.overallState, "ATTENTION_REQUIRED");
  assert.equal(model.primaryAction.label, "Resume Entries");
});

test("E: raw startup green cannot outrank detached backend truth", () => {
  const scenario = baseScenario();
  scenario.backendUrl = null;
  scenario.source.canRunLiveActions = false;
  scenario.source.apiReachable = false;
  scenario.source.healthReachable = false;
  scenario.source.mode = "backend_down";
  scenario.backend.state = "backend_down";
  scenario.backend.detail = "The desktop is not currently attached to the live dashboard API.";
  const model = deriveOperationalReadiness(scenario);

  assert.notEqual(model.overallState, "READY");
  assert.equal(model.launchAllowed, false);
  assert.equal(model.dashboardAttached, false);
  assert.match(model.primaryIssueTitle, /not fully attached/i);
});
