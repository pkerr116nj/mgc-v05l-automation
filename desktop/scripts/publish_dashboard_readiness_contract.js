"use strict";

const fs = require("node:fs");
const path = require("node:path");
const { spawnSync } = require("node:child_process");
const {
  buildDashboardReadinessContract,
  classifyDashboardReadiness,
} = require("./launch_readiness");

function parseArgs(argv) {
  const parsed = {};
  for (let index = 0; index < argv.length; index += 1) {
    const raw = String(argv[index] ?? "");
    if (!raw.startsWith("--")) {
      continue;
    }
    const body = raw.slice(2);
    const eqIndex = body.indexOf("=");
    if (eqIndex >= 0) {
      parsed[body.slice(0, eqIndex)] = body.slice(eqIndex + 1);
      continue;
    }
    const next = argv[index + 1];
    if (next !== undefined && !String(next).startsWith("--")) {
      parsed[body] = next;
      index += 1;
    } else {
      parsed[body] = "1";
    }
  }
  return parsed;
}

function sleepMs(ms) {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

function readJson(pathname, fallback = null) {
  try {
    return JSON.parse(fs.readFileSync(pathname, "utf8"));
  } catch {
    return fallback;
  }
}

function writeJson(pathname, payload) {
  fs.mkdirSync(path.dirname(pathname), { recursive: true });
  const tempPath = `${pathname}.tmp`;
  fs.writeFileSync(tempPath, `${JSON.stringify(payload, null, 2)}\n`);
  fs.renameSync(tempPath, pathname);
}

function desktopRuntimeRoot() {
  const explicit = String(process.env.MGC_DESKTOP_STATE_CACHE_ROOT || "").trim();
  if (explicit) {
    return explicit;
  }
  const home = String(process.env.HOME || "").trim();
  if (process.platform === "darwin" && home) {
    return path.join(home, "Library", "Application Support", "mgc-operator-desktop", "runtime");
  }
  if (home) {
    return path.join(home, ".cache", "mgc-operator-desktop", "runtime");
  }
  return path.join("/tmp", "mgc-operator-desktop-runtime");
}

function processExists(pid) {
  if (!pid || Number(pid) <= 0) {
    return false;
  }
  const result = spawnSync("ps", ["-p", String(pid)], {
    encoding: "utf8",
  });
  return (result.status ?? 1) === 0;
}

function listenerOwnerForConfiguredUrl(configuredUrl) {
  if (!configuredUrl) {
    return { pid: null, alive: false, command: null };
  }
  try {
    const parsed = new URL(configuredUrl);
    const port = parsed.port ? Number(parsed.port) : parsed.protocol === "https:" ? 443 : 80;
    if (!port) {
      return { pid: null, alive: false, command: null };
    }
    const pidResult = spawnSync("lsof", ["-nP", `-iTCP:${port}`, "-sTCP:LISTEN", "-t"], {
      encoding: "utf8",
    });
    const pid = Number(String(pidResult.stdout || "").trim().split(/\s+/)[0] || 0) || null;
    const alive = pid ? processExists(pid) : false;
    let command = null;
    if (pid) {
      const psResult = spawnSync("ps", ["-p", String(pid), "-o", "command="], {
        encoding: "utf8",
      });
      if ((psResult.status ?? 1) === 0) {
        command = String(psResult.stdout || "").trim() || null;
      }
    }
    return { pid, alive, command };
  } catch {
    return { pid: null, alive: false, command: null };
  }
}

async function probeJsonEndpoint(url, maxTimeSeconds) {
  const timeoutMs = Math.max(250, Number(maxTimeSeconds || 0) * 1000);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error(`timeout after ${timeoutMs}ms`)), timeoutMs);
  try {
    const response = await fetch(url, {
      method: "GET",
      headers: {
        accept: "application/json",
      },
      signal: controller.signal,
    });
    const stdout = String(await response.text()).trim();
    let parsedJson = null;
    let parseError = null;
    if (response.ok && stdout) {
      try {
        parsedJson = JSON.parse(stdout);
      } catch (error) {
        parseError = String(error);
      }
    }
    return {
      attempted: true,
      exit_status: response.ok ? 0 : response.status,
      ok: response.ok,
      stdout_preview: stdout ? stdout.slice(0, 500) : null,
      stderr: response.ok ? null : `${response.status} ${response.statusText}`.trim(),
      json_valid: parsedJson !== null,
      parse_error: parseError,
      parsed_json: parsedJson,
    };
  } catch (error) {
    return {
      attempted: true,
      exit_status: 1,
      ok: false,
      stdout_preview: null,
      stderr: String(error?.message || error || "unknown fetch failure"),
      json_valid: false,
      parse_error: null,
      parsed_json: null,
    };
  } finally {
    clearTimeout(timer);
  }
}

async function sampleDashboardState({ configuredUrl, infoFile }) {
  const dashboardInfo = infoFile ? readJson(infoFile, null) : null;
  const effectiveUrl = dashboardInfo?.url || configuredUrl || null;
  const managerSnapshotPath =
    String(process.env.MGC_OPERATOR_DASHBOARD_SNAPSHOT_PATH || "").trim() ||
    (infoFile ? path.resolve(path.dirname(infoFile), "..", "dashboard_api_snapshot.json") : null);
  const managerSnapshot = managerSnapshotPath ? readJson(managerSnapshotPath, null) : null;
  const dashboardInfoPid = Number(dashboardInfo?.pid || 0) || null;
  const listenerOwner = listenerOwnerForConfiguredUrl(effectiveUrl);

  let healthProbe = {
    attempted: false,
    listener_bound: false,
    exit_status: null,
    stdout_preview: null,
    stderr: null,
    json_valid: false,
    parse_error: null,
    parsed_json: null,
  };
  let dashboardProbe = {
    attempted: false,
    ok: false,
    exit_status: null,
    stdout_preview: null,
    stderr: null,
    json_valid: false,
    parse_error: null,
    parsed_json: null,
  };

  if (effectiveUrl) {
    try {
      const healthUrl = new URL("/health", effectiveUrl).toString();
      const dashboardUrl = new URL("/api/dashboard", effectiveUrl).toString();
      const health = await probeJsonEndpoint(healthUrl, 2);
      healthProbe = {
        attempted: true,
        listener_bound: health.ok,
        exit_status: health.exit_status,
        stdout_preview: health.stdout_preview,
        stderr: health.stderr,
        json_valid: health.json_valid,
        parse_error: health.parse_error,
        parsed_json: health.parsed_json,
      };
      if (health.ok) {
        dashboardProbe = await probeJsonEndpoint(dashboardUrl, 4);
      }
    } catch {
      healthProbe = {
        attempted: true,
        listener_bound: false,
        exit_status: -1,
        stdout_preview: null,
        stderr: "dashboard health probe could not be constructed",
        json_valid: false,
        parse_error: null,
        parsed_json: null,
      };
    }
  }

  return classifyDashboardReadiness({
    observedAt: new Date().toISOString(),
    configuredUrl: effectiveUrl,
    infoFilePresent: Boolean(dashboardInfo),
    infoFilePid: dashboardInfoPid,
    infoFilePidAlive: dashboardInfoPid ? processExists(dashboardInfoPid) : false,
    infoFileStartedAt: dashboardInfo?.started_at ?? null,
    infoFileBuildStamp: dashboardInfo?.build_stamp ?? null,
    infoFileInstanceId: dashboardInfo?.server_instance_id ?? dashboardInfo?.instance_id ?? null,
    listenerOwnerPid: listenerOwner.pid,
    listenerOwnerAlive: listenerOwner.alive,
    listenerOwnerCommand: listenerOwner.command,
    managerSnapshot,
    healthProbe,
    dashboardProbe,
  });
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const outputPath = String(args.output || "").trim();
  if (!outputPath) {
    console.error("Missing required --output for dashboard readiness contract publishing.");
    process.exit(1);
  }

  const infoFile = String(args["info-file"] || "").trim() || null;
  const configuredUrl = String(args["configured-url"] || "").trim() || null;
  const waitTimeoutMs = Math.max(0, Number(args["wait-timeout-ms"] || 0));
  const sampleIntervalMs = Math.max(100, Number(args["sample-interval-ms"] || 500));
  const minStableSamples = Math.max(2, Number(args["min-stable-samples"] || 3));
  const stabilityWindowMs = Math.max(500, Number(args["stability-window-ms"] || 1500));
  const leaseTtlMs = Math.max(sampleIntervalMs * 4, Number(args["lease-ttl-ms"] || 5000));
  const mode = String(args.mode || "startup_wait").trim() || "startup_wait";
  const startedAtMs = Date.now();
  const samples = [];
  const sampleHistoryLimit = Math.max(4, Number(args["sample-history-limit"] || 8));
  const managerPid = Number(args["manager-pid"] || 0) || null;
  const serverPid = Number(args["server-pid"] || 0) || null;
  const publisher = {
    source: "dashboard_manager",
    managerPid,
    managerMode: String(args["manager-mode"] || "").trim() || null,
    managerInstanceId: String(args["manager-instance-id"] || "").trim() || null,
    pid: process.pid,
  };
  const localReadinessMirrorPath = path.join(desktopRuntimeRoot(), "desktop_cache", "operator_dashboard_readiness.json");

  while (true) {
    if (managerPid && !processExists(managerPid)) {
      process.exit(3);
    }
    if (serverPid && !processExists(serverPid)) {
      process.exit(4);
    }
    samples.push(await sampleDashboardState({ configuredUrl, infoFile }));
    if (samples.length > sampleHistoryLimit) {
      samples.splice(0, samples.length - sampleHistoryLimit);
    }
    const contract = buildDashboardReadinessContract({
      configuredUrl,
      samples,
      sampleHistoryLimit,
      stabilityWindowMs,
      minStableSamples,
      sampleIntervalMs,
      leaseTtlMs,
      publisher,
    });
    writeJson(outputPath, contract);
    if (path.resolve(localReadinessMirrorPath) !== path.resolve(outputPath)) {
      writeJson(localReadinessMirrorPath, contract);
    }
    if (mode === "startup_wait" && contract.launch_allowed === true) {
      process.exit(0);
    }
    if (mode === "startup_wait" && Date.now() - startedAtMs >= waitTimeoutMs) {
      process.exit(2);
    }
    sleepMs(sampleIntervalMs);
  }
}

main().catch((error) => {
  console.error(String(error?.stack || error || "unknown readiness publisher failure"));
  process.exit(1);
});
