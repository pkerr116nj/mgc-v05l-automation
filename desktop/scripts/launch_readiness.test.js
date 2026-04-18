"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const {
  buildDashboardReadinessContract,
  classifyDashboardReadiness,
  resolveLaunchReadiness,
  verifyDashboardReadinessContract,
} = require("./launch_readiness");

function makeSample(overrides = {}) {
  return classifyDashboardReadiness({
    observedAt: overrides.observedAt ?? "2026-04-08T10:00:00.000Z",
    configuredUrl: "http://127.0.0.1:8790/",
    infoFilePresent: true,
    infoFilePid: 321,
    infoFilePidAlive: true,
    infoFileStartedAt: "2026-04-08T09:59:50.000Z",
    infoFileBuildStamp: "build-abc",
    infoFileInstanceId: "instance-1",
    listenerOwnerPid: 321,
    listenerOwnerAlive: true,
    listenerOwnerCommand: "python -m mgc_v05l.app.main operator-dashboard",
    healthProbe: {
      listener_bound: true,
      json_valid: true,
      parsed_json: {
        generated_at: overrides.healthGeneratedAt ?? overrides.observedAt ?? "2026-04-08T10:00:00.000Z",
        status: "ok",
        ready: true,
        phase: "stable_attached",
        dashboard_attached: true,
        paper_runtime_ready: true,
        pid: 321,
        build_stamp: "build-abc",
        instance_id: "instance-1",
        checks: {
          api_dashboard_responding: { ok: true },
          operator_surface_loadable: { ok: true },
          startup_convergence_stable: { ok: true },
        },
      },
    },
    dashboardProbe: {
      attempted: true,
      ok: true,
      json_valid: true,
      parsed_json: {
        generated_at: overrides.dashboardGeneratedAt ?? "2026-04-08T10:00:00.000Z",
        operator_surface: { ok: true },
        research_runtime_bridge: { ok: true },
        startup_control_plane: {
          overall_state: "READY",
          launch_allowed: true,
          summary_line: "Startup dependencies are aligned for paper-only launch.",
          primary_dependency_key: null,
          primary_reason: null,
          convergence: {
            phase: "stable_attached",
            reason: "Dashboard/API and tracked paper runtime remained attached long enough to be treated as stably ready.",
            stable_ready: true,
            dashboard_attached: true,
            paper_runtime_ready: true,
          },
          counts: {
            ready: 5,
            warming: 0,
            blocked: 0,
            degraded: 0,
            reconciliation_required: 0,
            needs_attention_now: 0,
          },
        },
        dashboard_meta: {
          build_stamp: "build-abc",
          server_pid: 321,
          server_instance_id: "instance-1",
        },
      },
    },
    ...overrides,
  });
}

function makeManagerSnapshot(overrides = {}) {
  return {
    generated_at: overrides.generated_at ?? new Date().toISOString(),
    dashboard_meta: {
      build_stamp: overrides.build_stamp ?? "build-abc",
      server_pid: overrides.server_pid ?? 321,
      server_instance_id: overrides.server_instance_id ?? "instance-1",
      server_url: overrides.server_url ?? "http://127.0.0.1:8790/",
    },
    operator_surface: { ok: true },
    startup_control_plane: {
      overall_state: overrides.overall_state ?? "READY",
      launch_allowed: overrides.launch_allowed ?? true,
      launch_candidate: overrides.launch_candidate ?? true,
      dependencies_aligned: overrides.dependencies_aligned ?? true,
      primary_reason: overrides.primary_reason ?? null,
      convergence: {
        phase: overrides.phase ?? "stable_attached",
        reason: overrides.reason ?? "Dashboard/API and tracked paper runtime remained attached long enough to be treated as stably ready.",
        reason_code: overrides.reason_code ?? "stable_attached",
        stable_ready: overrides.stable_ready ?? true,
        dashboard_attached: overrides.dashboard_attached ?? true,
        paper_runtime_ready: overrides.paper_runtime_ready ?? true,
      },
    },
  };
}

test("dead owner with surviving info-file signals is not launch-ready", () => {
  const sample = makeSample({
    infoFilePidAlive: false,
    listenerOwnerAlive: false,
    healthProbe: { listener_bound: false, json_valid: false, parsed_json: null },
    dashboardProbe: { attempted: false, ok: false, json_valid: false, parsed_json: null },
  });
  assert.equal(sample.readiness_state, "NOT_READY");
  assert.equal(sample.reason_code, "listener_not_started");
});

test("health green but wrong payload identity is classified as identity mismatch", () => {
  const sample = makeSample({
    dashboardProbe: {
      attempted: true,
      ok: true,
      json_valid: true,
      parsed_json: {
        operator_surface: { ok: true },
        research_runtime_bridge: { ok: true },
        startup_control_plane: {
          overall_state: "READY",
          launch_allowed: true,
          summary_line: "Startup dependencies are aligned for paper-only launch.",
          counts: { ready: 5, warming: 0, blocked: 0, degraded: 0, reconciliation_required: 0, needs_attention_now: 0 },
        },
        dashboard_meta: {
          build_stamp: "build-abc",
          server_pid: 321,
          server_instance_id: "instance-2",
        },
      },
    },
  });
  assert.equal(sample.readiness_state, "AMBIGUOUS");
  assert.equal(sample.reason_code, "identity_mismatch");
});

test("cold-start stale prior payload is treated as service warming instead of ownership mismatch", () => {
  const sample = makeSample({
    healthProbe: {
      listener_bound: true,
      json_valid: true,
      parsed_json: {
        status: "starting",
        ready: false,
        pid: 321,
        build_stamp: "build-abc",
        instance_id: "instance-1",
        checks: {
          api_dashboard_responding: { ok: true },
          operator_surface_loadable: { ok: true },
        },
      },
    },
    dashboardProbe: {
      attempted: true,
      ok: true,
      json_valid: true,
      parsed_json: {
        generated_at: "2026-04-08T09:59:59.000Z",
        operator_surface: { ok: true },
        startup_control_plane: {
          overall_state: "READY",
          launch_allowed: true,
          summary_line: "Startup dependencies are aligned for paper-only launch.",
          convergence: {
            phase: "stable_attached",
            reason: "Dashboard/API and tracked paper runtime remained attached long enough to be treated as stably ready.",
            stable_ready: true,
            dashboard_attached: true,
            paper_runtime_ready: true,
          },
          counts: {
            ready: 5,
            warming: 0,
            blocked: 0,
            degraded: 0,
            reconciliation_required: 0,
            needs_attention_now: 0,
          },
        },
        dashboard_meta: {
          build_stamp: "build-abc",
          server_pid: 999,
          server_instance_id: "instance-prior",
        },
      },
    },
  });
  assert.equal(sample.readiness_state, "NOT_READY");
  assert.equal(sample.reason_code, "service_warming");
});

test("current-instance stable payload can be treated as ready before health catch-up completes", () => {
  const sample = makeSample({
    healthProbe: {
      listener_bound: true,
      json_valid: true,
      parsed_json: {
        status: "starting",
        ready: false,
        pid: 321,
        build_stamp: "build-abc",
        instance_id: "instance-1",
        checks: {
          api_dashboard_responding: { ok: false },
          operator_surface_loadable: { ok: false },
        },
      },
    },
  });
  assert.equal(sample.readiness_state, "READY");
  assert.equal(sample.reason_code, "payload_ready");
});

test("current-instance payload clears passive stability-window warmup when attach truth is already healthy", () => {
  const sample = makeSample({
    healthProbe: {
      listener_bound: true,
      json_valid: true,
      parsed_json: {
        status: "starting",
        ready: false,
        pid: 321,
        build_stamp: "build-abc",
        instance_id: "instance-1",
        checks: {
          api_dashboard_responding: { ok: true },
          operator_surface_loadable: { ok: true },
          startup_convergence_stable: { ok: false },
        },
      },
    },
    dashboardProbe: {
      attempted: true,
      ok: true,
      json_valid: true,
      parsed_json: {
        generated_at: "2026-04-08T10:00:00.000Z",
        operator_surface: { ok: true },
        research_runtime_bridge: { ok: true },
        startup_control_plane: {
          overall_state: "WARMING",
          launch_allowed: false,
          launch_candidate: true,
          dependencies_aligned: true,
          primary_dependency_key: "dashboard_backend",
          primary_reason_code: "stability_window_incomplete",
          primary_reason: "Tracked paper runtime is verified, but the manager stability window has not completed yet.",
          convergence: {
            phase: "paper_runtime_verified",
            reason: "Tracked paper runtime is verified, but the manager stability window has not completed yet.",
            reason_code: "stability_window_incomplete",
            stable_ready: false,
            dashboard_attached: true,
            paper_runtime_ready: true,
          },
          counts: {
            ready: 5,
            warming: 0,
            blocked: 0,
            degraded: 0,
            reconciliation_required: 0,
            needs_attention_now: 0,
          },
        },
        dashboard_meta: {
          build_stamp: "build-abc",
          server_pid: 321,
          server_instance_id: "instance-1",
        },
      },
    },
  });
  assert.equal(sample.readiness_state, "READY");
  assert.equal(sample.reason_code, "payload_ready");
});

test("health green but payload not ready remains not launch-ready", () => {
  const sample = makeSample({
    dashboardProbe: {
      attempted: true,
      ok: true,
      json_valid: true,
      parsed_json: {
        dashboard_meta: {
          build_stamp: "build-abc",
          server_pid: 321,
          server_instance_id: "instance-1",
        },
      },
    },
  });
  assert.equal(sample.readiness_state, "NOT_READY");
  assert.equal(sample.reason_code, "payload_not_ready");
});

test("A: manager stable READY is treated as READY even when secondary payload probing lags", () => {
  const sample = classifyDashboardReadiness({
    observedAt: "2026-04-08T10:00:00.000Z",
    configuredUrl: "http://127.0.0.1:8790/",
    infoFilePresent: true,
    infoFilePid: 321,
    infoFilePidAlive: true,
    infoFileStartedAt: "2026-04-08T09:59:50.000Z",
    infoFileBuildStamp: "build-abc",
    infoFileInstanceId: "instance-1",
    listenerOwnerPid: 321,
    listenerOwnerAlive: true,
    listenerOwnerCommand: "python -m mgc_v05l.app.main operator-dashboard",
    managerSnapshot: makeManagerSnapshot(),
    healthProbe: {
      listener_bound: true,
      json_valid: true,
      parsed_json: {
        status: "ok",
        ready: true,
        pid: 321,
        build_stamp: "build-abc",
        instance_id: "instance-1",
        checks: {
          api_dashboard_responding: { ok: true },
          operator_surface_loadable: { ok: true },
        },
      },
    },
    dashboardProbe: {
      attempted: true,
      ok: false,
      json_valid: false,
      parsed_json: null,
    },
  });
  assert.equal(sample.readiness_state, "READY");
  assert.equal(sample.reason_code, "manager_snapshot_ready");
  assert.equal(sample.payload.source, "MANAGER_SNAPSHOT");
});

test("manager stable READY tolerates a transient listener probe miss when ownership still agrees", () => {
  const sample = classifyDashboardReadiness({
    observedAt: "2026-04-08T10:00:00.000Z",
    configuredUrl: "http://127.0.0.1:8790/",
    infoFilePresent: true,
    infoFilePid: 321,
    infoFilePidAlive: true,
    infoFileStartedAt: "2026-04-08T09:59:50.000Z",
    infoFileBuildStamp: "build-abc",
    infoFileInstanceId: "instance-1",
    listenerOwnerPid: 321,
    listenerOwnerAlive: true,
    listenerOwnerCommand: "python -m mgc_v05l.app.main operator-dashboard",
    managerSnapshot: makeManagerSnapshot(),
    healthProbe: {
      listener_bound: false,
      json_valid: false,
      parsed_json: null,
    },
    dashboardProbe: {
      attempted: false,
      ok: false,
      json_valid: false,
      parsed_json: null,
    },
  });
  assert.equal(sample.readiness_state, "READY");
  assert.equal(sample.reason_code, "manager_snapshot_ready");
});

test("manager stable READY tolerates a healthy current-instance snapshot older than 15 seconds", () => {
  const generatedAt = new Date(Date.now() - 45_000).toISOString();
  const sample = classifyDashboardReadiness({
    observedAt: new Date().toISOString(),
    configuredUrl: "http://127.0.0.1:8790/",
    infoFilePresent: true,
    infoFilePid: 321,
    infoFilePidAlive: true,
    infoFileStartedAt: "2026-04-08T09:59:50.000Z",
    infoFileBuildStamp: "build-abc",
    infoFileInstanceId: "instance-1",
    listenerOwnerPid: 321,
    listenerOwnerAlive: true,
    listenerOwnerCommand: "python -m mgc_v05l.app.main operator-dashboard",
    managerSnapshot: makeManagerSnapshot({ generated_at: generatedAt }),
    healthProbe: {
      listener_bound: false,
      json_valid: false,
      parsed_json: null,
    },
    dashboardProbe: {
      attempted: false,
      ok: false,
      json_valid: false,
      parsed_json: null,
    },
  });
  assert.equal(sample.readiness_state, "READY");
  assert.equal(sample.reason_code, "manager_snapshot_ready");
  assert.equal(sample.manager_truth.ready, true);
});

test("B: manager stable READY can still be held by launcher policy for a specific explicit reason", () => {
  const resolved = resolveLaunchReadiness({
    contract: null,
    managerSnapshot: makeManagerSnapshot(),
    configuredUrl: null,
    infoFilePid: 321,
    infoFilePidAlive: true,
    infoFileBuildStamp: "build-abc",
    infoFileInstanceId: "instance-1",
    listenerOwnerAlive: true,
    maxAgeMs: 15000,
  });
  assert.equal(resolved.ready, false);
  assert.equal(resolved.classification, "MANAGER_READY_POLICY_HOLD");
  assert.equal(resolved.reason_code, "configured_url_missing");
  assert.equal(resolved.truth_source, "MANAGER_SNAPSHOT");
});

test("C: manager warming remains non-ready", () => {
  const sample = classifyDashboardReadiness({
    observedAt: "2026-04-08T10:00:00.000Z",
    configuredUrl: "http://127.0.0.1:8790/",
    infoFilePresent: true,
    infoFilePid: 321,
    infoFilePidAlive: true,
    infoFileBuildStamp: "build-abc",
    infoFileInstanceId: "instance-1",
    listenerOwnerPid: 321,
    listenerOwnerAlive: true,
    managerSnapshot: makeManagerSnapshot({
      overall_state: "WARMING",
      launch_allowed: false,
      launch_candidate: true,
      dependencies_aligned: true,
      stable_ready: false,
      phase: "paper_runtime_verified",
      reason_code: "stability_window_incomplete",
      reason: "Tracked paper runtime is verified, but the manager stability window has not completed yet.",
    }),
    healthProbe: {
      listener_bound: true,
      json_valid: true,
      parsed_json: {
        status: "starting",
        ready: false,
        pid: 321,
        build_stamp: "build-abc",
        instance_id: "instance-1",
        checks: {
          api_dashboard_responding: { ok: true },
          operator_surface_loadable: { ok: true },
        },
      },
    },
    dashboardProbe: {
      attempted: false,
      ok: false,
      json_valid: false,
      parsed_json: null,
    },
  });
  assert.equal(sample.readiness_state, "NOT_READY");
  assert.equal(sample.reason_code, "service_warming");
});

test("D: manager READY wins over a stale secondary publisher contract", () => {
  const staleContract = buildDashboardReadinessContract({
    generatedAt: "2026-04-08T09:59:00.000Z",
    samples: [
      makeSample({ observedAt: "2026-04-08T09:58:58.000Z" }),
      makeSample({ observedAt: "2026-04-08T09:58:59.000Z" }),
      makeSample({ observedAt: "2026-04-08T09:59:00.000Z" }),
    ],
    stabilityWindowMs: 1500,
    minStableSamples: 3,
    sampleIntervalMs: 500,
  });
  const resolved = resolveLaunchReadiness({
    contract: staleContract,
    managerSnapshot: makeManagerSnapshot({ generated_at: new Date().toISOString() }),
    configuredUrl: "http://127.0.0.1:8790/",
    infoFilePid: 321,
    infoFilePidAlive: true,
    infoFileBuildStamp: "build-abc",
    infoFileInstanceId: "instance-1",
    listenerOwnerAlive: true,
    maxAgeMs: 15000,
  });
  assert.equal(resolved.ready, true);
  assert.equal(resolved.truth_source, "MANAGER_SNAPSHOT");
  assert.equal(resolved.reason_code, "manager_snapshot_ready");
});

test("launcher resolution accepts stale manager snapshot when fresh live health keeps manager truth current", () => {
  const staleSnapshotAt = "2026-04-08T09:59:00.000Z";
  const resolved = resolveLaunchReadiness({
    contract: {
      generated_at: staleSnapshotAt,
      readiness_state: "READY",
      reason_code: "ready",
      reason_detail: "Old secondary publisher contract.",
      launch_allowed: true,
      configured_url: "http://127.0.0.1:8790/",
      ownership: {
        info_file_pid_alive: true,
        listener_owner_alive: true,
        owner_signals_agree: true,
        owner_pid_matches_info: true,
        owner_pid_matches_health: true,
        owner_pid_matches_payload: true,
        identity_signals_agree: true,
        instance_id_matches_all: true,
        build_stamp_matches_all: true,
      },
      lease: {
        expires_at: "2026-04-08T10:30:00.000Z",
      },
    },
    managerSnapshot: makeManagerSnapshot({
      generated_at: staleSnapshotAt,
    }),
    healthPayload: {
      generated_at: new Date().toISOString(),
      status: "ok",
      ready: true,
      pid: 321,
      build_stamp: "build-abc",
      instance_id: "instance-1",
      checks: {
        api_dashboard_responding: { ok: true },
        operator_surface_loadable: { ok: true },
        startup_convergence_stable: { ok: true },
      },
    },
    configuredUrl: "http://127.0.0.1:8790/",
    infoFilePid: 321,
    infoFilePidAlive: true,
    infoFileBuildStamp: "build-abc",
    infoFileInstanceId: "instance-1",
    listenerOwnerAlive: true,
    maxAgeMs: 15000,
  });
  assert.equal(resolved.ready, true);
  assert.equal(resolved.truth_source, "MANAGER_SNAPSHOT");
  assert.equal(resolved.reason_code, "manager_snapshot_ready");
});

test("E: no green-vs-ambiguous contradiction remains once manager is truly stable", () => {
  const resolved = resolveLaunchReadiness({
    contract: {
      readiness_state: "AMBIGUOUS",
      reason_code: "stability_window_not_met",
      reason_detail: "Secondary publisher lagged.",
      launch_allowed: false,
    },
    managerSnapshot: makeManagerSnapshot(),
    configuredUrl: "http://127.0.0.1:8790/",
    infoFilePid: 321,
    infoFilePidAlive: true,
    infoFileBuildStamp: "build-abc",
    infoFileInstanceId: "instance-1",
    listenerOwnerAlive: true,
    maxAgeMs: 15000,
  });
  assert.equal(resolved.ready, true);
  assert.equal(resolved.classification, "MANAGER_SNAPSHOT_READY");
  assert.equal(resolved.truth_source, "MANAGER_SNAPSHOT");
});

test("manager snapshot ready detail explains when it overrides a stale secondary contract", () => {
  const staleContract = buildDashboardReadinessContract({
    generatedAt: "2026-04-08T09:59:00.000Z",
    samples: [
      makeSample({ observedAt: "2026-04-08T09:58:58.000Z" }),
      makeSample({ observedAt: "2026-04-08T09:58:59.000Z" }),
      makeSample({ observedAt: "2026-04-08T09:59:00.000Z" }),
    ],
    stabilityWindowMs: 1500,
    minStableSamples: 3,
    sampleIntervalMs: 500,
  });
  const resolved = resolveLaunchReadiness({
    contract: staleContract,
    managerSnapshot: makeManagerSnapshot(),
    healthPayload: {
      generated_at: new Date().toISOString(),
      status: "ok",
      ready: true,
      pid: 321,
      build_stamp: "build-abc",
      instance_id: "instance-1",
      checks: {
        api_dashboard_responding: { ok: true },
        operator_surface_loadable: { ok: true },
        startup_convergence_stable: { ok: true },
      },
    },
    configuredUrl: "http://127.0.0.1:8790/",
    infoFilePid: 321,
    infoFilePidAlive: true,
    infoFileBuildStamp: "build-abc",
    infoFileInstanceId: "instance-1",
    listenerOwnerAlive: true,
    maxAgeMs: 1000,
  });
  assert.equal(resolved.ready, true);
  assert.equal(resolved.classification, "MANAGER_SNAPSHOT_READY");
  assert.match(String(resolved.reason_detail), /dashboard_contract_stale/);
});

test("manager heartbeat keeps manager READY authoritative even when the cached snapshot timestamp is older", () => {
  const now = Date.now();
  const observedAt = new Date(now).toISOString();
  const staleSnapshotAt = new Date(now - 20000).toISOString();
  const sample = classifyDashboardReadiness({
    observedAt,
    configuredUrl: "http://127.0.0.1:8790/",
    infoFilePresent: true,
    infoFilePid: 321,
    infoFilePidAlive: true,
    infoFileStartedAt: "2026-04-08T09:59:50.000Z",
    infoFileBuildStamp: "build-abc",
    infoFileInstanceId: "instance-1",
    listenerOwnerPid: 321,
    listenerOwnerAlive: true,
    managerSnapshot: makeManagerSnapshot({
      generated_at: staleSnapshotAt,
    }),
    managerSnapshotMaxAgeMs: 5000,
    healthProbe: {
      listener_bound: true,
      json_valid: true,
      parsed_json: {
        generated_at: observedAt,
        status: "ok",
        ready: true,
        phase: "stable_attached",
        dashboard_attached: true,
        paper_runtime_ready: true,
        pid: 321,
        build_stamp: "build-abc",
        instance_id: "instance-1",
        checks: {
          api_dashboard_responding: { ok: true },
          operator_surface_loadable: { ok: true },
          startup_convergence_stable: { ok: true },
        },
      },
    },
    dashboardProbe: {
      attempted: true,
      ok: false,
      json_valid: false,
      parsed_json: null,
    },
  });
  assert.equal(sample.readiness_state, "READY");
  assert.equal(sample.reason_code, "manager_snapshot_ready");
  assert.equal(sample.manager_truth.snapshot_fresh, false);
  assert.equal(sample.manager_truth.heartbeat_authoritative, true);
  assert.equal(sample.payload.source, "MANAGER_SNAPSHOT");
});

test("health green and payload valid still refuses when startup dependencies are not ready", () => {
  const sample = makeSample({
    dashboardProbe: {
      attempted: true,
      ok: true,
      json_valid: true,
      parsed_json: {
        generated_at: "2026-04-08T10:00:00.000Z",
        operator_surface: { ok: true },
        research_runtime_bridge: { ok: true },
        startup_control_plane: {
          overall_state: "BLOCKED",
          launch_allowed: false,
          summary_line: "Startup is blocked because Paper Runtime is blocked.",
          primary_dependency_key: "paper_runtime",
          primary_reason: "Paper runtime is not yet active.",
          convergence: {
            phase: "runtime_attachment_verified",
            reason: "Tracked paper runtime is attached, but still blocked, halted, or reconciling.",
            stable_ready: false,
            dashboard_attached: true,
            paper_runtime_ready: false,
          },
          counts: {
            ready: 2,
            warming: 0,
            blocked: 2,
            degraded: 0,
            reconciliation_required: 1,
            needs_attention_now: 2,
          },
        },
        dashboard_meta: {
          build_stamp: "build-abc",
          server_pid: 321,
          server_instance_id: "instance-1",
        },
      },
    },
  });
  assert.equal(sample.readiness_state, "NOT_READY");
  assert.equal(sample.reason_code, "startup_dependencies_not_ready");
  assert.equal(sample.control_plane.launch_allowed, false);
  assert.equal(sample.control_plane.primary_dependency_key, "paper_runtime");
});

test("payload with contradictory stable flags still refuses launch", () => {
  const sample = makeSample({
    dashboardProbe: {
      attempted: true,
      ok: true,
      json_valid: true,
      parsed_json: {
        generated_at: "2026-04-08T10:00:00.000Z",
        operator_surface: { ok: true },
        research_runtime_bridge: { ok: true },
        startup_control_plane: {
          overall_state: "READY",
          launch_allowed: true,
          summary_line: "Startup dependencies are aligned for paper-only launch.",
          convergence: {
            phase: "paper_runtime_verified",
            reason: "Manager stability window has not completed yet.",
            stable_ready: false,
            dashboard_attached: true,
            paper_runtime_ready: true,
          },
          counts: { ready: 5, warming: 0, blocked: 0, degraded: 0, reconciliation_required: 0, needs_attention_now: 0 },
        },
        dashboard_meta: {
          build_stamp: "build-abc",
          server_pid: 321,
          server_instance_id: "instance-1",
        },
      },
    },
  });
  assert.equal(sample.readiness_state, "NOT_READY");
  assert.equal(sample.reason_code, "startup_dependencies_not_ready");
  assert.equal(sample.control_plane.convergence_stable_ready, false);
});

test("ownership flips during the stability window keep launch_allowed false", () => {
  const contract = buildDashboardReadinessContract({
    samples: [
      makeSample({ observedAt: "2026-04-08T10:00:00.000Z" }),
      makeSample({
        observedAt: "2026-04-08T10:00:00.600Z",
        listenerOwnerPid: 654,
      }),
      makeSample({ observedAt: "2026-04-08T10:00:01.200Z" }),
    ],
    stabilityWindowMs: 1000,
    minStableSamples: 3,
    sampleIntervalMs: 500,
  });
  assert.equal(contract.launch_allowed, false);
  assert.equal(contract.reason_code, "stability_window_not_met");
});

test("short-window oscillation remains refused even when one probe turns green", () => {
  const contract = buildDashboardReadinessContract({
    samples: [
      makeSample({
        observedAt: "2026-04-08T10:00:00.000Z",
        dashboardProbe: { attempted: false, ok: false, json_valid: false, parsed_json: null },
      }),
      makeSample({ observedAt: "2026-04-08T10:00:00.500Z" }),
      makeSample({
        observedAt: "2026-04-08T10:00:01.000Z",
        dashboardProbe: { attempted: false, ok: false, json_valid: false, parsed_json: null },
      }),
    ],
    stabilityWindowMs: 1000,
    minStableSamples: 3,
    sampleIntervalMs: 500,
  });
  assert.equal(contract.launch_allowed, false);
  assert.equal(contract.reason_code, "stability_window_not_met");
});

test("readiness contract heals after recent samples stabilize again", () => {
  const contract = buildDashboardReadinessContract({
    sampleHistoryLimit: 4,
    samples: [
      makeSample({
        observedAt: "2026-04-08T10:00:00.000Z",
        healthProbe: {
          listener_bound: false,
          json_valid: false,
          parsed_json: null,
        },
        dashboardProbe: { attempted: false, ok: false, json_valid: false, parsed_json: null },
      }),
      makeSample({ observedAt: "2026-04-08T10:00:01.000Z" }),
      makeSample({ observedAt: "2026-04-08T10:00:02.000Z" }),
      makeSample({ observedAt: "2026-04-08T10:00:03.000Z" }),
    ],
    stabilityWindowMs: 1500,
    minStableSamples: 3,
    sampleIntervalMs: 1000,
  });
  assert.equal(contract.launch_allowed, true);
  assert.equal(contract.reason_code, "ready");
});

test("true launch_allowed requires agreeing owner, health, payload, and identity over the stability window", () => {
  const contract = buildDashboardReadinessContract({
    samples: [
      makeSample({ observedAt: "2026-04-08T10:00:00.000Z" }),
      makeSample({ observedAt: "2026-04-08T10:00:00.800Z" }),
      makeSample({ observedAt: "2026-04-08T10:00:01.600Z" }),
    ],
    stabilityWindowMs: 1500,
    minStableSamples: 3,
    sampleIntervalMs: 500,
  });
  assert.equal(contract.launch_allowed, true);
  assert.equal(contract.reason_code, "ready");
  assert.equal(contract.ownership.owner_signals_agree, true);
  assert.equal(contract.ownership.identity_signals_agree, true);
  assert.equal(contract.control_plane.launch_allowed, true);
  assert.equal(contract.payload.startup_control_plane_present, true);
});

test("payload_ready bypass allows launch when only passive manager warmup remains", () => {
  const readyWarmupSample = makeSample({
    healthProbe: {
      listener_bound: true,
      json_valid: true,
      parsed_json: {
        status: "starting",
        ready: false,
        pid: 321,
        build_stamp: "build-abc",
        instance_id: "instance-1",
        checks: {
          api_dashboard_responding: { ok: true },
          operator_surface_loadable: { ok: true },
          startup_convergence_stable: { ok: false },
        },
      },
    },
    dashboardProbe: {
      attempted: true,
      ok: true,
      json_valid: true,
      parsed_json: {
        generated_at: "2026-04-08T10:00:00.000Z",
        operator_surface: { ok: true },
        research_runtime_bridge: { ok: true },
        startup_control_plane: {
          overall_state: "WARMING",
          launch_allowed: false,
          launch_candidate: true,
          dependencies_aligned: true,
          primary_dependency_key: "dashboard_backend",
          primary_reason_code: "stability_window_incomplete",
          primary_reason: "Tracked paper runtime is verified, but the manager stability window has not completed yet.",
          convergence: {
            phase: "paper_runtime_verified",
            reason: "Tracked paper runtime is verified, but the manager stability window has not completed yet.",
            reason_code: "stability_window_incomplete",
            stable_ready: false,
            dashboard_attached: true,
            paper_runtime_ready: true,
          },
        },
        dashboard_meta: {
          build_stamp: "build-abc",
          server_pid: 321,
          server_instance_id: "instance-1",
        },
      },
    },
  });
  const contract = buildDashboardReadinessContract({
    samples: [readyWarmupSample],
    stabilityWindowMs: 1500,
    minStableSamples: 3,
    sampleIntervalMs: 500,
  });
  assert.equal(contract.launch_allowed, true);
  assert.equal(contract.readiness_state, "READY");
  assert.equal(contract.reason_code, "payload_ready");
});

test("launcher verification refuses stale contracts even if they were once launch-allowed", () => {
  const contract = buildDashboardReadinessContract({
    generatedAt: "2026-04-08T09:59:00.000Z",
    samples: [
      makeSample({ observedAt: "2026-04-08T09:58:58.000Z" }),
      makeSample({ observedAt: "2026-04-08T09:58:59.000Z" }),
      makeSample({ observedAt: "2026-04-08T09:59:00.000Z" }),
    ],
    stabilityWindowMs: 1500,
    minStableSamples: 3,
    sampleIntervalMs: 500,
  });
  const verification = verifyDashboardReadinessContract(contract, {
    maxAgeMs: 1000,
    configuredUrl: "http://127.0.0.1:8790/",
  });
  assert.equal(verification.ready, false);
  assert.equal(verification.reason_code, "dashboard_contract_stale");
});

test("launcher verification accepts healthy contracts inside the steady-state freshness window", () => {
  const contract = buildDashboardReadinessContract({
    generatedAt: new Date(Date.now() - 45_000).toISOString(),
    samples: [
      makeSample({ observedAt: new Date(Date.now() - 47_000).toISOString() }),
      makeSample({ observedAt: new Date(Date.now() - 46_000).toISOString() }),
      makeSample({ observedAt: new Date(Date.now() - 45_000).toISOString() }),
    ],
    stabilityWindowMs: 1500,
    minStableSamples: 3,
    sampleIntervalMs: 500,
    leaseTtlMs: 120_000,
  });
  const verification = verifyDashboardReadinessContract(contract, {
    configuredUrl: "http://127.0.0.1:8790/",
  });
  assert.equal(verification.ready, true);
  assert.equal(verification.classification, "READY");
});

test("healthy manager contract includes a renewable lease and current generation", () => {
  const contract = buildDashboardReadinessContract({
    generatedAt: "2026-04-08T10:00:02.000Z",
    samples: [
      makeSample({ observedAt: "2026-04-08T10:00:00.000Z" }),
      makeSample({ observedAt: "2026-04-08T10:00:01.000Z" }),
      makeSample({ observedAt: "2026-04-08T10:00:02.000Z" }),
    ],
    stabilityWindowMs: 1500,
    minStableSamples: 3,
    sampleIntervalMs: 500,
    leaseTtlMs: 6000,
    publisher: {
      managerInstanceId: "manager-gen-1",
    },
  });
  assert.equal(contract.lease.lease_ttl_ms, 6000);
  assert.equal(contract.lease.owner_generation, "manager-gen-1");
  assert.equal(contract.launch_allowed, true);
});

test("lease expiry refuses a once-fresh launch-ready contract", () => {
  const contract = buildDashboardReadinessContract({
    generatedAt: "2026-04-08T10:00:02.000Z",
    samples: [
      makeSample({ observedAt: "2026-04-08T10:00:00.000Z" }),
      makeSample({ observedAt: "2026-04-08T10:00:01.000Z" }),
      makeSample({ observedAt: "2026-04-08T10:00:02.000Z" }),
    ],
    stabilityWindowMs: 1500,
    minStableSamples: 3,
    sampleIntervalMs: 500,
    leaseTtlMs: 1000,
  });
  const originalNow = Date.now;
  Date.now = () => Date.parse("2026-04-08T10:00:04.500Z");
  try {
    const verification = verifyDashboardReadinessContract(contract, {
      maxAgeMs: 15000,
      configuredUrl: "http://127.0.0.1:8790/",
    });
    assert.equal(verification.ready, false);
    assert.equal(verification.reason_code, "dashboard_contract_lease_expired");
  } finally {
    Date.now = originalNow;
  }
});

test("restart publishes a new owner generation cleanly", () => {
  const previous = buildDashboardReadinessContract({
    generatedAt: "2026-04-08T10:00:02.000Z",
    samples: [
      makeSample({ observedAt: "2026-04-08T10:00:00.000Z" }),
      makeSample({ observedAt: "2026-04-08T10:00:01.000Z" }),
      makeSample({ observedAt: "2026-04-08T10:00:02.000Z" }),
    ],
    publisher: { managerInstanceId: "manager-gen-1" },
  });
  const restarted = buildDashboardReadinessContract({
    generatedAt: "2026-04-08T10:05:02.000Z",
    samples: [
      makeSample({
        observedAt: "2026-04-08T10:05:00.000Z",
        infoFilePid: 654,
        listenerOwnerPid: 654,
        healthProbe: {
          listener_bound: true,
          json_valid: true,
          parsed_json: {
            status: "ok",
            ready: true,
            pid: 654,
            build_stamp: "build-abc",
            instance_id: "instance-2",
            checks: {
              api_dashboard_responding: { ok: true },
              operator_surface_loadable: { ok: true },
            },
          },
        },
        dashboardProbe: {
          attempted: true,
          ok: true,
          json_valid: true,
          parsed_json: {
            operator_surface: { ok: true },
            research_runtime_bridge: { ok: true },
            startup_control_plane: {
              overall_state: "READY",
              launch_allowed: true,
              summary_line: "Startup dependencies are aligned for paper-only launch.",
              counts: { ready: 5, warming: 0, blocked: 0, degraded: 0, reconciliation_required: 0, needs_attention_now: 0 },
            },
            dashboard_meta: {
              build_stamp: "build-abc",
              server_pid: 654,
              server_instance_id: "instance-2",
            },
          },
        },
        infoFileInstanceId: "instance-2",
      }),
      makeSample({
        observedAt: "2026-04-08T10:05:01.000Z",
        infoFilePid: 654,
        listenerOwnerPid: 654,
        infoFileInstanceId: "instance-2",
        healthProbe: {
          listener_bound: true,
          json_valid: true,
          parsed_json: {
            status: "ok",
            ready: true,
            pid: 654,
            build_stamp: "build-abc",
            instance_id: "instance-2",
            checks: {
              api_dashboard_responding: { ok: true },
              operator_surface_loadable: { ok: true },
            },
          },
        },
        dashboardProbe: {
          attempted: true,
          ok: true,
          json_valid: true,
          parsed_json: {
            operator_surface: { ok: true },
            research_runtime_bridge: { ok: true },
            startup_control_plane: {
              overall_state: "READY",
              launch_allowed: true,
              summary_line: "Startup dependencies are aligned for paper-only launch.",
              counts: { ready: 5, warming: 0, blocked: 0, degraded: 0, reconciliation_required: 0, needs_attention_now: 0 },
            },
            dashboard_meta: {
              build_stamp: "build-abc",
              server_pid: 654,
              server_instance_id: "instance-2",
            },
          },
        },
      }),
      makeSample({
        observedAt: "2026-04-08T10:05:02.000Z",
        infoFilePid: 654,
        listenerOwnerPid: 654,
        infoFileInstanceId: "instance-2",
        healthProbe: {
          listener_bound: true,
          json_valid: true,
          parsed_json: {
            status: "ok",
            ready: true,
            pid: 654,
            build_stamp: "build-abc",
            instance_id: "instance-2",
            checks: {
              api_dashboard_responding: { ok: true },
              operator_surface_loadable: { ok: true },
            },
          },
        },
        dashboardProbe: {
          attempted: true,
          ok: true,
          json_valid: true,
          parsed_json: {
            operator_surface: { ok: true },
            research_runtime_bridge: { ok: true },
            startup_control_plane: {
              overall_state: "READY",
              launch_allowed: true,
              summary_line: "Startup dependencies are aligned for paper-only launch.",
              counts: { ready: 5, warming: 0, blocked: 0, degraded: 0, reconciliation_required: 0, needs_attention_now: 0 },
            },
            dashboard_meta: {
              build_stamp: "build-abc",
              server_pid: 654,
              server_instance_id: "instance-2",
            },
          },
        },
      }),
    ],
    publisher: { managerInstanceId: "manager-gen-2" },
  });
  assert.notEqual(previous.lease.owner_generation, restarted.lease.owner_generation);
  assert.notEqual(previous.ownership.info_file_instance_id, restarted.ownership.info_file_instance_id);
});
