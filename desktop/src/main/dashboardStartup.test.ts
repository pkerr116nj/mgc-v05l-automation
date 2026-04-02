import test from "node:test";
import assert from "node:assert/strict";

import {
  classifyStartupFailure,
  isRecoverableLocalDashboardListener,
  shouldAutoReconnectDashboardFailure,
} from "./dashboardStartup";

test("classifies stale listener conflict from launcher markers", () => {
  const result = classifyStartupFailure(
    [
      "STARTUP_FAILURE_KIND=stale_listener_conflict",
      "STARTUP_NEXT_ACTION=Stop the conflicting listener or retry after stale cleanup.",
      "STARTUP_STALE_LISTENER_DETECTED=1",
    ].join("\n"),
  );
  assert.equal(result.kind, "stale_listener_conflict");
  assert.equal(result.staleListenerDetected, true);
  assert.equal(result.hint, "Stop the conflicting listener or retry after stale cleanup.");
});

test("classifies stale dashboard instance from launcher markers", () => {
  const result = classifyStartupFailure(
    [
      "STARTUP_FAILURE_KIND=stale_dashboard_instance",
      "STARTUP_NEXT_ACTION=Stop the stale local dashboard process, then retry.",
      "STARTUP_STALE_LISTENER_DETECTED=1",
    ].join("\n"),
  );
  assert.equal(result.kind, "stale_dashboard_instance");
  assert.equal(result.staleListenerDetected, true);
});

test("classifies build mismatch from launcher markers", () => {
  const result = classifyStartupFailure(
    [
      "STARTUP_FAILURE_KIND=build_mismatch",
      "STARTUP_HEALTH_REACHABLE=1",
      "STARTUP_BUILD_MISMATCH=1",
    ].join("\n"),
  );
  assert.equal(result.kind, "build_mismatch");
  assert.equal(result.healthReachable, true);
  assert.equal(result.buildMismatchDetected, true);
});

test("classifies health-up dashboard-api timeout deterministically", () => {
  const result = classifyStartupFailure(
    "Live /health is reachable but /api/dashboard did not become responsive before timeout.",
    { healthReachable: true, dashboardApiTimedOut: true },
  );
  assert.equal(result.kind, "dashboard_api_not_ready");
  assert.equal(result.healthReachable, true);
  assert.equal(result.dashboardApiTimedOut, true);
});

test("classifies early managed-process exit", () => {
  const result = classifyStartupFailure("Dashboard failed to start: server process exited early.");
  assert.equal(result.kind, "early_process_exit");
  assert.equal(result.earlyProcessExitDetected, true);
});

test("classifies permission or bind failures deterministically", () => {
  const result = classifyStartupFailure("Operation not permitted while binding localhost listener.");
  assert.equal(result.kind, "permission_or_bind_failure");
});

test("classifies environment/bootstrap failure", () => {
  const result = classifyStartupFailure("Schwab auth bootstrap incomplete: missing SCHWAB_APP_KEY.");
  assert.equal(result.kind, "environment_failure");
});

test("detects recoverable local dashboard listener commands", () => {
  assert.equal(
    isRecoverableLocalDashboardListener("/Users/patrick/Documents/MGC-v05l-automation/.venv/bin/python -m mgc_v05l.app.main operator-dashboard --host 127.0.0.1 --port 8790"),
    true,
  );
  assert.equal(isRecoverableLocalDashboardListener("python -m http.server 8790"), false);
});

test("only retries reconnect for transient startup failures", () => {
  assert.equal(
    shouldAutoReconnectDashboardFailure(classifyStartupFailure("Dashboard failed to start: server process exited early."), 0),
    true,
  );
  assert.equal(
    shouldAutoReconnectDashboardFailure(classifyStartupFailure("STARTUP_FAILURE_KIND=stale_listener_conflict\nSTARTUP_STALE_LISTENER_DETECTED=1"), 0),
    true,
  );
  assert.equal(
    shouldAutoReconnectDashboardFailure(classifyStartupFailure("Live /health is reachable but /api/dashboard did not become responsive before timeout.", { dashboardApiTimedOut: true }), 0),
    false,
  );
  assert.equal(
    shouldAutoReconnectDashboardFailure(classifyStartupFailure("STARTUP_FAILURE_KIND=stale_listener_conflict"), 0),
    true,
  );
  assert.equal(
    shouldAutoReconnectDashboardFailure(classifyStartupFailure("STARTUP_FAILURE_KIND=stale_listener_conflict"), 1),
    false,
  );
  assert.equal(shouldAutoReconnectDashboardFailure(classifyStartupFailure("Dashboard failed to start: server process exited early."), 3), false);
});
