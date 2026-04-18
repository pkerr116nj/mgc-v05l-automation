const fs = require("node:fs");
const path = require("node:path");
const { randomUUID } = require("node:crypto");
const { spawnSync } = require("node:child_process");
const {
  resolveLaunchReadiness,
  summarizeReadinessSamples,
  verifyDashboardReadinessContract,
} = require("./launch_readiness");

const desktopRoot = path.resolve(__dirname, "..");
const releaseRoot = path.join(desktopRoot, "release", "local");
const targetApp = path.join(releaseRoot, "MGC Operator.app");
const infoPlistPath = path.join(targetApp, "Contents", "Info.plist");
const resourcesAppDir = path.join(targetApp, "Contents", "Resources", "app");
const preferredExecutablePath = path.join(targetApp, "Contents", "MacOS", "MGC Operator");
const launchLockPath = path.join(releaseRoot, ".local_app_launch.lock");
const launchCooldownPath = path.join(releaseRoot, ".local_app_launch.cooldown.json");
const repoRoot = path.resolve(desktopRoot, "..");
const operatorRuntimeRoot = path.join(repoRoot, "outputs", "operator_dashboard", "runtime");
const serviceFirstOperatorConsoleScript = path.join(repoRoot, "scripts", "run_supervised_paper_operator_console.sh");
const launchAttemptLogPath = path.join(operatorRuntimeRoot, "desktop_launch_attempts.jsonl");
const latestLaunchAttemptPath = path.join(operatorRuntimeRoot, "desktop_launch_attempt_latest.json");
const startupStatusPath = path.join(operatorRuntimeRoot, "desktop_electron_startup.json");
const startupEventsPath = path.join(operatorRuntimeRoot, "desktop_electron_startup_events.jsonl");
const latestStartupAbortPath = path.join(operatorRuntimeRoot, "desktop_startup_abort_latest.json");
const startupAbortLogPath = path.join(operatorRuntimeRoot, "desktop_startup_aborts.jsonl");
const dashboardInfoPath = path.join(operatorRuntimeRoot, "operator_dashboard.json");
const dashboardReadinessContractPath = path.join(operatorRuntimeRoot, "operator_dashboard_readiness.json");
const dashboardSnapshotPath = path.join(repoRoot, "outputs", "operator_dashboard", "dashboard_api_snapshot.json");
const LOCK_WAIT_TIMEOUT_MS = 120000;
const LOCK_STALE_TIMEOUT_MS = 180000;
const AUTOMATION_LAUNCH_DEBOUNCE_MS = 15000;
const STARTUP_ABORT_HOLDOFF_MS = 60000;
const EXISTING_PROCESS_WAIT_TIMEOUT_MS = 30000;
const DASHBOARD_READY_WAIT_TIMEOUT_MS = 10000;
const DASHBOARD_READINESS_CONTRACT_MAX_AGE_MS = 60000;
const OPEN_LAUNCH_STARTUP_TIMEOUT_MS = 20000;

function launchViaServiceFirstOperatorConsole() {
  const result = spawnSync("bash", [serviceFirstOperatorConsoleScript, ...process.argv.slice(2)], {
    cwd: repoRoot,
    env: sanitizedLaunchEnvironment(),
    stdio: "inherit",
  });
  process.exit(result.status ?? 1);
}

if (process.env.MGC_DESKTOP_USE_LEGACY_LAUNCHER !== "1") {
  launchViaServiceFirstOperatorConsole();
}

function sleepMs(ms) {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

function bundleLooksReady() {
  return (
    fs.existsSync(targetApp) &&
    fs.existsSync(infoPlistPath) &&
    fs.existsSync(resourcesAppDir) &&
    fs.existsSync(preferredExecutablePath)
  );
}

function ensureLocalBundle() {
  if (bundleLooksReady()) {
    return;
  }
  const result = spawnSync(process.execPath, [path.join(__dirname, "package_local_app.js")], {
    cwd: desktopRoot,
    env: process.env,
    stdio: "inherit",
  });
  if ((result.status ?? 1) !== 0) {
    process.exit(result.status ?? 1);
  }
}

function acquireLaunchLock() {
  fs.mkdirSync(releaseRoot, { recursive: true });
  const startedAt = Date.now();
  while (true) {
    try {
      const fd = fs.openSync(launchLockPath, "wx");
      fs.writeFileSync(
        fd,
        JSON.stringify({ pid: process.pid, started_at: new Date().toISOString(), app: targetApp }, null, 2),
      );
      fs.closeSync(fd);
      return;
    } catch (error) {
      if (error?.code !== "EEXIST") {
        throw error;
      }
      try {
        const lockPayload = readJson(launchLockPath, null);
        if (lockPayload?.pid && !processExists(Number(lockPayload.pid))) {
          fs.rmSync(launchLockPath, { force: true });
          continue;
        }
        const stat = fs.statSync(launchLockPath);
        if (Date.now() - stat.mtimeMs > LOCK_STALE_TIMEOUT_MS) {
          fs.rmSync(launchLockPath, { force: true });
          continue;
        }
      } catch {
        continue;
      }
      if (Date.now() - startedAt > LOCK_WAIT_TIMEOUT_MS) {
        throw new Error(`Timed out waiting for launch lock at ${launchLockPath}`);
      }
      sleepMs(500);
    }
  }
}

function releaseLaunchLock() {
  fs.rmSync(launchLockPath, { force: true });
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
  fs.writeFileSync(pathname, `${JSON.stringify(payload, null, 2)}\n`);
}

function appendJsonl(pathname, payload) {
  fs.mkdirSync(path.dirname(pathname), { recursive: true });
  fs.appendFileSync(pathname, `${JSON.stringify(payload)}\n`);
}

function normalizeOptionalPath(value) {
  if (!value) {
    return null;
  }
  return path.isAbsolute(value) ? value : path.resolve(repoRoot, value);
}

function sanitizedLaunchEnvironment(overrides = {}) {
  const env = {
    ...process.env,
    ...overrides,
  };
  delete env.CODEX_SANDBOX;
  delete env.CODEX_SHELL;
  return env;
}

function readJsonl(pathname) {
  if (!fs.existsSync(pathname)) {
    return [];
  }
  return String(fs.readFileSync(pathname, "utf8") || "")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      try {
        return JSON.parse(line);
      } catch {
        return null;
      }
    })
    .filter(Boolean);
}

function pushArg(args, name, value) {
  if (value === undefined || value === null || value === "") {
    return;
  }
  args.push(`--${name}=${value}`);
}

function directLaunchExecutable() {
  if (!fs.existsSync(preferredExecutablePath)) {
    throw new Error(`Packaged executable is missing at ${preferredExecutablePath}`);
  }
  return preferredExecutablePath;
}

function listExistingAppProcesses() {
  const result = spawnSync("ps", ["-ef"], {
    cwd: desktopRoot,
    env: process.env,
    encoding: "utf8",
  });
  if ((result.status ?? 1) !== 0) {
    return [];
  }
  return String(result.stdout || "")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const parts = line.split(/\s+/, 8);
      const pid = Number(parts[1] || 0);
      const command = parts[7] || "";
      if (!pid || !command) {
        return null;
      }
      const match = command.match(/^(.*)$/);
      if (!match) {
        return null;
      }
      return { pid, command };
    })
    .filter(Boolean)
    .filter((row) => row.pid !== process.pid)
    .filter((row) => {
      const command = row.command || "";
      return (
        command.includes("/MGC Operator.app/Contents/MacOS/MGC Operator") ||
        command.includes("/Electron.app/Contents/MacOS/Electron") ||
        /\/Contents\/MacOS\/MGC Operator(?:\s|$)/.test(command) ||
        /\/Contents\/MacOS\/Electron(?:\s|$)/.test(command)
      );
    });
}

function processExists(pid) {
  if (!pid || Number(pid) <= 0) {
    return false;
  }
  const result = spawnSync("ps", ["-p", String(pid)], {
    cwd: desktopRoot,
    env: process.env,
    encoding: "utf8",
  });
  if (result.error?.code === "EPERM") {
    return true;
  }
  return (result.status ?? 1) === 0;
}

function processCommand(pid) {
  if (!pid || Number(pid) <= 0) {
    return null;
  }
  const result = spawnSync("ps", ["-p", String(pid), "-o", "command="], {
    cwd: desktopRoot,
    env: process.env,
    encoding: "utf8",
  });
  if ((result.status ?? 1) !== 0) {
    return null;
  }
  const value = String(result.stdout || "").trim();
  return value || null;
}

function fetchJsonSync(url, timeoutSeconds = 5) {
  if (!url) {
    return null;
  }
  const result = spawnSync("curl", ["-sS", "--max-time", String(timeoutSeconds), url], {
    cwd: desktopRoot,
    env: process.env,
    encoding: "utf8",
  });
  if ((result.status ?? 1) !== 0) {
    return null;
  }
  try {
    return JSON.parse(String(result.stdout || ""));
  } catch {
    return null;
  }
}

function relevantLaunchEnvironment() {
  const capturePath = normalizeOptionalPath(process.env.MGC_DESKTOP_CAPTURE_PATH);
  const names = [
    "MGC_DESKTOP_CAPTURE_PATH",
    "MGC_DESKTOP_CAPTURE_HASH",
    "MGC_DESKTOP_CAPTURE_DELAY_MS",
    "MGC_DESKTOP_CAPTURE_AND_EXIT",
    "MGC_DESKTOP_DIRECT_EXEC",
    "MGC_DESKTOP_DIRECT_EXEC_DIAGNOSTIC",
    "MGC_DESKTOP_USE_LEGACY_LAUNCHER",
    "MGC_OPERATOR_DASHBOARD_URL",
    "MGC_REPO_ROOT",
    "TMPDIR",
  ];
  return Object.fromEntries(
    names.map((name) => [
      name,
      name === "MGC_DESKTOP_CAPTURE_PATH" ? capturePath : process.env[name] ?? null,
    ]),
  );
}

function waitForNoExistingAppProcesses() {
  const startedAt = Date.now();
  while (true) {
    const rows = listExistingAppProcesses();
    if (!rows.length) {
      return { waitedMs: Date.now() - startedAt, rows: [] };
    }
    if (Date.now() - startedAt > EXISTING_PROCESS_WAIT_TIMEOUT_MS) {
      return { waitedMs: Date.now() - startedAt, rows };
    }
    sleepMs(500);
  }
}

function maybeDebounceAutomationLaunch() {
  const previous = readJson(launchCooldownPath, null);
  if (!previous || !previous.completed_at_ms) {
    return 0;
  }
  const remaining = AUTOMATION_LAUNCH_DEBOUNCE_MS - (Date.now() - Number(previous.completed_at_ms));
  if (remaining > 0) {
    sleepMs(remaining);
    return remaining;
  }
  return 0;
}

function recentStartupAbortHoldoffState() {
  const latestAbort = readJson(latestStartupAbortPath, null);
  if (!latestAbort || latestAbort.failure_class !== "MACOS_STARTUP_ABORT_PRE_JS") {
    return {
      active: false,
      remainingMs: 0,
      latestAbort: latestAbort || null,
    };
  }
  const recordedAtMs = Date.parse(String(latestAbort.recorded_at || ""));
  if (!Number.isFinite(recordedAtMs)) {
    return {
      active: false,
      remainingMs: 0,
      latestAbort,
    };
  }
  const remainingMs = STARTUP_ABORT_HOLDOFF_MS - (Date.now() - recordedAtMs);
  return {
    active: remainingMs > 0,
    remainingMs: Math.max(0, remainingMs),
    latestAbort,
  };
}

function dashboardReadinessContractState() {
  const dashboardInfo = readJson(dashboardInfoPath, null);
  const configuredUrl = dashboardInfo?.url || process.env.MGC_OPERATOR_DASHBOARD_URL || null;
  const contract = readJson(dashboardReadinessContractPath, null);
  const liveHealth = configuredUrl ? fetchJsonSync(`${String(configuredUrl).replace(/\/$/, "")}/health`) : null;
  const liveDashboard = configuredUrl ? fetchJsonSync(`${String(configuredUrl).replace(/\/$/, "")}/api/dashboard`, 10) : null;
  const managerSnapshot = liveDashboard || readJson(dashboardSnapshotPath, null);
  const currentInfoPid =
    dashboardInfo?.pid
    || liveHealth?.pid
    || contract?.ownership?.info_file_pid
    || null;
  const currentListenerOwnerPid =
    liveHealth?.pid
    || dashboardInfo?.pid
    || contract?.ownership?.listener_owner_pid
    || null;
  const currentInfoPidAlive = currentInfoPid ? processExists(currentInfoPid) : false;
  const currentListenerOwnerAlive = currentListenerOwnerPid ? processExists(currentListenerOwnerPid) : false;
  const baseVerification = verifyDashboardReadinessContract(contract, {
    maxAgeMs: DASHBOARD_READINESS_CONTRACT_MAX_AGE_MS,
    configuredUrl,
  });
  const verification =
    baseVerification.ready === true && (!currentInfoPidAlive || !currentListenerOwnerAlive)
      ? {
          ...baseVerification,
          ready: false,
          classification: "DASHBOARD_CONTRACT_OWNER_EXITED",
          reason_code: "dashboard_contract_owner_exited",
          reason_detail: "The published readiness contract was fresh, but its owner process was no longer alive at launch time.",
        }
      : baseVerification;
  const resolved = resolveLaunchReadiness({
    contract,
    managerSnapshot,
    healthPayload: liveHealth || contract?.health || null,
    configuredUrl,
    infoFilePid: currentInfoPid,
    infoFilePidAlive: currentInfoPidAlive,
    infoFileBuildStamp: dashboardInfo?.build_stamp ?? null,
    infoFileInstanceId: dashboardInfo?.server_instance_id ?? dashboardInfo?.instance_id ?? null,
    listenerOwnerAlive: currentListenerOwnerAlive,
    maxAgeMs: DASHBOARD_READINESS_CONTRACT_MAX_AGE_MS,
  });
  return {
    configured_url: configuredUrl,
    contract_present: Boolean(contract),
    contract_path: dashboardReadinessContractPath,
    contract_generated_at: contract?.generated_at ?? null,
    contract_launch_allowed: contract?.launch_allowed === true,
    contract_reason_code: contract?.reason_code ?? null,
    contract_reason_detail: contract?.reason_detail ?? null,
    contract_owner_pid_alive_now: currentInfoPidAlive,
    contract_listener_owner_alive_now: currentListenerOwnerAlive,
    contract_age_ms: resolved.contract_age_ms ?? verification.contract_age_ms ?? null,
    contract_stale_override_active:
      resolved.ready === true && resolved.truth_source === "MANAGER_SNAPSHOT" && baseVerification.ready !== true,
    contract_override_reason_code:
      resolved.ready === true && resolved.truth_source === "MANAGER_SNAPSHOT" && baseVerification.ready !== true
        ? (baseVerification.reason_code ?? null)
        : null,
    ...resolved,
  };
}

function waitForDashboardReadinessContract() {
  const startedAt = Date.now();
  let latestState = dashboardReadinessContractState();
  const samples = [sampleDashboardReadinessContract(latestState)];
  while (true) {
    if (latestState.ready === true) {
      return {
        waitedMs: Date.now() - startedAt,
        state: latestState,
        ready: true,
        readyStreak: 1,
        sampleSummary: summarizeReadinessSamples(samples),
        samples,
      };
    }
    if (Date.now() - startedAt > DASHBOARD_READY_WAIT_TIMEOUT_MS) {
      return {
        waitedMs: Date.now() - startedAt,
        state: latestState,
        ready: false,
        readyStreak: 0,
        sampleSummary: summarizeReadinessSamples(samples),
        samples,
      };
    }
    sleepMs(500);
    latestState = dashboardReadinessContractState();
    samples.push(sampleDashboardReadinessContract(latestState));
  }
}

function sampleDashboardReadinessContract(state) {
  return {
    observed_at: new Date().toISOString(),
    readiness_state: state.ready === true ? "READY" : state.freshness_ok === false ? "NOT_READY" : "AMBIGUOUS",
    classification: state.classification,
    ready: state.ready === true,
    reason_code: state.reason_code,
    contract_generated_at: state.contract_generated_at ?? null,
    contract_launch_allowed: state.contract_launch_allowed === true,
    contract_age_ms: state.contract_age_ms ?? null,
  };
}

function writeStartupPlaceholder({ attemptId, launchMode, executablePath }) {
  const capturePath = normalizeOptionalPath(process.env.MGC_DESKTOP_CAPTURE_PATH);
  writeJson(startupStatusPath, {
    recorded_at: new Date().toISOString(),
    launch_attempt_id: attemptId,
    startup_writer: "launcher",
    stage: "launcher:awaiting-electron-js-startup",
    launch_mode: launchMode,
    executable_path: executablePath,
    repo_root: process.env.MGC_REPO_ROOT || repoRoot,
    dashboard_url: process.env.MGC_OPERATOR_DASHBOARD_URL || null,
    capture_requested: Boolean(capturePath),
    js_startup_observed: false,
    window_observed: false,
    renderer_dom_ready: false,
    renderer_did_finish_load: false,
    capture_written: false,
  });
}

function loadStartupArtifacts(attemptId) {
  const status = readJson(startupStatusPath, null);
  const eventRows = readJsonl(startupEventsPath).filter((row) => String(row.launch_attempt_id || "") === String(attemptId || ""));
  const latestEvent = eventRows.length ? eventRows[eventRows.length - 1] : null;
  const startupStatusMatches = Boolean(status && String(status.launch_attempt_id || "") === String(attemptId || ""));
  const jsStartupObserved =
    Boolean(latestEvent?.js_startup_observed) ||
    Boolean(latestEvent?.startup_writer === "electron") ||
    Boolean(status?.js_startup_observed) ||
    Boolean(status?.startup_writer === "electron");
  return {
    startup_status_observed: startupStatusMatches,
    js_startup_observed: jsStartupObserved,
    startup_status: startupStatusMatches ? status : null,
    startup_events: eventRows,
    startup_event_count: eventRows.length,
    startup_latest_event: latestEvent,
    startup_window_observed: Boolean(latestEvent?.window_observed || status?.window_observed),
    startup_renderer_dom_ready: Boolean(latestEvent?.renderer_dom_ready || status?.renderer_dom_ready),
    startup_renderer_loaded: Boolean(latestEvent?.renderer_did_finish_load || status?.renderer_did_finish_load),
    startup_capture_written: Boolean(latestEvent?.capture_written || status?.capture_written),
  };
}

function waitForStartupArtifacts(attemptId, timeoutMs) {
  const startedAt = Date.now();
  while (Date.now() - startedAt <= timeoutMs) {
    const artifacts = loadStartupArtifacts(attemptId);
    if (artifacts.js_startup_observed || artifacts.startup_event_count > 0) {
      return {
        waitedMs: Date.now() - startedAt,
        timedOut: false,
        ...artifacts,
      };
    }
    const latestAbort = readJson(latestStartupAbortPath, null);
    if (latestAbort && String(latestAbort.attempt_id || "") === String(attemptId || "")) {
      return {
        waitedMs: Date.now() - startedAt,
        timedOut: false,
        ...artifacts,
      };
    }
    sleepMs(250);
  }
  return {
    waitedMs: timeoutMs,
    timedOut: true,
    ...loadStartupArtifacts(attemptId),
  };
}

function deriveStartupBoundary(startupArtifacts) {
  const rows = Array.isArray(startupArtifacts?.startup_events) ? startupArtifacts.startup_events : [];
  const status = startupArtifacts?.startup_status || null;
  const stageRows = rows.map((row) => String(row?.stage || ""));
  const latestStage = String(startupArtifacts?.startup_latest_event?.stage || status?.stage || "");
  const stageSet = new Set(stageRows.filter(Boolean));
  if (latestStage) {
    stageSet.add(latestStage);
  }
  const sawStage = (prefix) => Array.from(stageSet).some((stage) => stage === prefix || stage.startsWith(`${prefix}:`));
  return {
    js_main_entry_observed: rows.some((row) => row?.js_main_entry_observed === true) || status?.js_main_entry_observed === true,
    will_finish_launching_observed: sawStage("app:will-finish-launching"),
    when_ready_observed: sawStage("whenReady"),
    browser_window_creation_started: sawStage("createWindow:start"),
    browser_window_constructed: sawStage("createWindow:browser-window-constructed"),
    preload_bridge_observed: rows.some((row) => String(row?.stage || "").startsWith("preload:")),
    latest_stage: latestStage || null,
  };
}

function maybeWriteStartupAbortArtifact(completedAttempt) {
  const isStartupAbort =
    completedAttempt.exit_signal === "SIGABRT" &&
    completedAttempt.likely_failure_zone === "APPKIT_LAUNCHSERVICES_PRE_JS";
  if (!isStartupAbort) {
    return;
  }
  const artifact = {
    recorded_at: new Date().toISOString(),
    failure_class: "MACOS_STARTUP_ABORT_PRE_JS",
    classification: "MACOS_STARTUP_ABORT_PRE_JS",
    retry_policy: "NO_AUTOMATIC_RETRY_HOLDOFF",
    recommended_holdoff_ms: STARTUP_ABORT_HOLDOFF_MS,
    operator_message:
      "Packaged app aborted before Electron JS startup at the macOS LaunchServices/AppKit boundary. This is not a dashboard-readiness failure.",
    attempt_id: completedAttempt.attempt_id,
    launch_mode: completedAttempt.launch_mode,
    executable_path: completedAttempt.executable_path,
    app_path: completedAttempt.app_path,
    cwd: desktopRoot,
    launcher_pid: completedAttempt.launcher_pid,
    launcher_parent_pid: completedAttempt.launcher_parent_pid,
    launcher_parent_command: completedAttempt.launcher_parent_command,
    relevant_env: completedAttempt.relevant_env,
    exit_status: completedAttempt.exit_status,
    exit_signal: completedAttempt.exit_signal,
    likely_failure_zone: completedAttempt.likely_failure_zone,
    died_before_js_startup: completedAttempt.died_before_js_startup,
    startup_boundary: completedAttempt.startup_boundary,
    startup_status_observed: completedAttempt.startup_status_observed,
    startup_event_count: completedAttempt.startup_event_count,
    startup_status: completedAttempt.startup_status || null,
    startup_latest_event: completedAttempt.startup_latest_event || null,
    renderer_bootstrap_reached: completedAttempt.js_startup_observed === true,
    browser_window_creation_started: completedAttempt.startup_boundary?.browser_window_creation_started === true,
    browser_window_constructed: completedAttempt.startup_boundary?.browser_window_constructed === true,
    preload_bridge_observed: completedAttempt.startup_boundary?.preload_bridge_observed === true,
    dashboard_readiness_verification_after_wait: completedAttempt.dashboard_readiness_verification_after_wait || null,
  };
  writeJson(latestStartupAbortPath, artifact);
  appendJsonl(startupAbortLogPath, artifact);
}

function currentLaunchAttemptBase({ attemptId, launchMode, executablePath, shouldUseDirectExecutable, forwardedArgs, openArgs }) {
  const dashboardInfo = readJson(dashboardInfoPath, null);
  const dashboardReadinessContract = dashboardReadinessContractState();
  const capturePath = normalizeOptionalPath(process.env.MGC_DESKTOP_CAPTURE_PATH);
  return {
    attempt_id: attemptId,
    recorded_at: new Date().toISOString(),
    launcher_pid: process.pid,
    launcher_parent_pid: process.ppid,
    launcher_parent_command: processCommand(process.ppid),
    app_path: targetApp,
    executable_path: executablePath,
    launch_mode: launchMode,
    direct_executable: shouldUseDirectExecutable,
    capture_requested: Boolean(capturePath),
    capture_path: capturePath,
    capture_hash: process.env.MGC_DESKTOP_CAPTURE_HASH || null,
    repo_root: process.env.MGC_REPO_ROOT || repoRoot,
    dashboard_url: dashboardInfo?.url || process.env.MGC_OPERATOR_DASHBOARD_URL || null,
    renderer_url: process.env.MGC_RENDERER_URL || process.env.VITE_DEV_SERVER_URL || null,
    relevant_env: relevantLaunchEnvironment(),
    forwarded_args: forwardedArgs,
    launch_args: shouldUseDirectExecutable ? openArgs.slice(3) : openArgs,
    dashboard_info: dashboardInfo,
    dashboard_readiness_contract: dashboardReadinessContract.contract || null,
    dashboard_readiness_verification: dashboardReadinessContract,
  };
}

function main() {
  acquireLaunchLock();
  try {
    ensureLocalBundle();
    const forwardedArgs = process.argv.slice(2);
    const attemptId = randomUUID();
    const shouldUseDirectExecutable = process.env.MGC_DESKTOP_DIRECT_EXEC_DIAGNOSTIC === "1";
    const executablePath = shouldUseDirectExecutable ? directLaunchExecutable() : targetApp;
    const launchMode = shouldUseDirectExecutable ? "DIRECT_EXECUTABLE_DIAGNOSTIC" : "OPEN_BUNDLE_AUTOMATION";
    const capturePath = normalizeOptionalPath(process.env.MGC_DESKTOP_CAPTURE_PATH);
    const openArgs = ["-n", targetApp, "--args", ...forwardedArgs];
    pushArg(openArgs, "mgc-capture-path", capturePath);
    pushArg(openArgs, "mgc-capture-hash", process.env.MGC_DESKTOP_CAPTURE_HASH);
    pushArg(openArgs, "mgc-capture-delay-ms", process.env.MGC_DESKTOP_CAPTURE_DELAY_MS);
    pushArg(openArgs, "mgc-capture-and-exit", process.env.MGC_DESKTOP_CAPTURE_AND_EXIT);
    pushArg(openArgs, "mgc-capture-js", process.env.MGC_DESKTOP_CAPTURE_JS);
    pushArg(openArgs, "mgc-capture-scroll-section-title", process.env.MGC_DESKTOP_CAPTURE_SCROLL_SECTION_TITLE);
    pushArg(openArgs, "mgc-capture-scroll-row-text", process.env.MGC_DESKTOP_CAPTURE_SCROLL_ROW_TEXT);
    pushArg(openArgs, "mgc-capture-window-width", process.env.MGC_DESKTOP_CAPTURE_WINDOW_WIDTH);
    pushArg(openArgs, "mgc-capture-window-height", process.env.MGC_DESKTOP_CAPTURE_WINDOW_HEIGHT);
    pushArg(openArgs, "mgc-renderer-url", process.env.MGC_RENDERER_URL || process.env.VITE_DEV_SERVER_URL);
    pushArg(openArgs, "mgc-repo-root", process.env.MGC_REPO_ROOT || repoRoot);
    pushArg(openArgs, "mgc-dashboard-url", process.env.MGC_OPERATOR_DASHBOARD_URL);
    pushArg(openArgs, "mgc-launch-attempt-id", attemptId);
    pushArg(openArgs, "mgc-launch-mode", launchMode);
    pushArg(openArgs, "mgc-executable-path", preferredExecutablePath);
    const debounceWaitMs = shouldUseDirectExecutable ? maybeDebounceAutomationLaunch() : 0;
    const existingProcessWait = shouldUseDirectExecutable ? waitForNoExistingAppProcesses() : { waitedMs: 0, rows: [] };
    const attemptBase = currentLaunchAttemptBase({
      attemptId,
      launchMode,
      executablePath,
      shouldUseDirectExecutable,
      forwardedArgs,
      openArgs,
    });
    const dashboardReadyWait = shouldUseDirectExecutable
      ? waitForDashboardReadinessContract()
      : {
          waitedMs: 0,
          state: attemptBase.dashboard_readiness_verification,
          ready: true,
          readyStreak: 1,
        };
    const prelaunchAttempt = {
      ...attemptBase,
      debounce_wait_ms: debounceWaitMs,
      existing_process_wait_ms: existingProcessWait.waitedMs,
      existing_processes_after_wait: existingProcessWait.rows,
      dashboard_ready_wait_ms: dashboardReadyWait.waitedMs,
      dashboard_ready_streak: dashboardReadyWait.readyStreak ?? 0,
      dashboard_readiness_summary: dashboardReadyWait.sampleSummary ?? null,
      dashboard_readiness_samples: dashboardReadyWait.samples ?? [],
      dashboard_contract_after_wait: dashboardReadyWait.state?.contract ?? null,
      dashboard_readiness_verification_after_wait: dashboardReadyWait.state,
      phase: "PRELAUNCH",
    };
    writeJson(latestLaunchAttemptPath, prelaunchAttempt);
    appendJsonl(launchAttemptLogPath, prelaunchAttempt);
    if (shouldUseDirectExecutable && existingProcessWait.rows.length > 0) {
      const failedAttempt = {
        ...prelaunchAttempt,
        phase: "ABORTED_PRELAUNCH",
        completed_at: new Date().toISOString(),
        completed_at_ms: Date.now(),
        exit_status: 1,
        reason: "existing_packaged_process_still_running",
      };
      writeJson(latestLaunchAttemptPath, failedAttempt);
      appendJsonl(launchAttemptLogPath, failedAttempt);
      writeJson(launchCooldownPath, failedAttempt);
      console.error("Refusing to launch packaged app while an existing packaged Electron process is still running.");
      process.exit(1);
    }
    const startupAbortHoldoff = shouldUseDirectExecutable
      ? recentStartupAbortHoldoffState()
      : { active: false, remainingMs: 0, latestAbort: null };
    if (shouldUseDirectExecutable && startupAbortHoldoff.active) {
      const failedAttempt = {
        ...prelaunchAttempt,
        phase: "ABORTED_PRELAUNCH",
        completed_at: new Date().toISOString(),
        completed_at_ms: Date.now(),
        exit_status: 1,
        reason: "recent_macos_startup_abort_holdoff",
        failure_class: "MACOS_STARTUP_ABORT_PRE_JS",
        readiness_classification: "MACOS_STARTUP_ABORT_HOLDOFF",
        refusal_reason_detail:
          "A recent packaged launch hit the pre-JS macOS startup-abort family. Holding off further automated packaged launches instead of relabeling it as readiness failure.",
        holdoff_remaining_ms: startupAbortHoldoff.remainingMs,
        latest_startup_abort: startupAbortHoldoff.latestAbort,
      };
      writeJson(latestLaunchAttemptPath, failedAttempt);
      appendJsonl(launchAttemptLogPath, failedAttempt);
      writeJson(launchCooldownPath, failedAttempt);
      console.error("Refusing to relaunch packaged automation during macOS startup-abort holdoff.");
      process.exit(1);
    }
    if (shouldUseDirectExecutable && dashboardReadyWait.ready !== true) {
      const failedAttempt = {
        ...prelaunchAttempt,
        phase: "ABORTED_PRELAUNCH",
        completed_at: new Date().toISOString(),
        completed_at_ms: Date.now(),
        exit_status: 1,
        reason: String(dashboardReadyWait.state?.reason_code || "dashboard_contract_not_ready"),
        refusal_reason_detail: String(
          dashboardReadyWait.state?.reason_detail || "Dashboard readiness contract is missing, stale, or not launch-allowed.",
        ),
        readiness_classification: String(dashboardReadyWait.state?.classification || "DASHBOARD_CONTRACT_NOT_READY"),
      };
      writeJson(latestLaunchAttemptPath, failedAttempt);
      appendJsonl(launchAttemptLogPath, failedAttempt);
      writeJson(launchCooldownPath, failedAttempt);
      console.error(`Refusing to launch packaged app for automation because dashboard readiness is ambiguous: ${failedAttempt.reason}.`);
      process.exit(1);
    }

    writeStartupPlaceholder({ attemptId, launchMode, executablePath });

    const launchEnv = {
      ...sanitizedLaunchEnvironment({
        MGC_REPO_ROOT: process.env.MGC_REPO_ROOT || repoRoot,
        MGC_DESKTOP_LAUNCH_ATTEMPT_ID: attemptId,
        MGC_DESKTOP_LAUNCH_MODE: launchMode,
        MGC_DESKTOP_EXECUTABLE_PATH: executablePath,
        MGC_DESKTOP_LAUNCH_ATTEMPT_LOG_PATH: launchAttemptLogPath,
        MGC_DESKTOP_LATEST_LAUNCH_PATH: latestLaunchAttemptPath,
        ...(capturePath ? { MGC_DESKTOP_CAPTURE_PATH: capturePath } : {}),
      }),
    };

    const child = shouldUseDirectExecutable
      ? spawnSync(executablePath, openArgs.slice(3), {
          cwd: desktopRoot,
          env: launchEnv,
          stdio: "inherit",
        })
      : spawnSync("open", openArgs, {
          cwd: desktopRoot,
          env: launchEnv,
          encoding: "utf8",
        });
    const openLaunchArtifacts = shouldUseDirectExecutable ? null : waitForStartupArtifacts(attemptId, OPEN_LAUNCH_STARTUP_TIMEOUT_MS);
    const completedAttempt = {
      ...attemptBase,
      phase: "COMPLETED",
      completed_at: new Date().toISOString(),
      completed_at_ms: Date.now(),
      debounce_wait_ms: debounceWaitMs,
      existing_process_wait_ms: existingProcessWait.waitedMs,
      dashboard_ready_wait_ms: dashboardReadyWait.waitedMs,
      dashboard_ready_streak: dashboardReadyWait.readyStreak ?? 0,
      dashboard_contract_after_wait: dashboardReadyWait.state?.contract ?? null,
      dashboard_readiness_verification_after_wait: dashboardReadyWait.state,
      exit_status: child.status ?? 0,
      exit_signal: child.signal ?? null,
      launcher_stdout: shouldUseDirectExecutable ? null : String(child.stdout || "").trim() || null,
      launcher_stderr: shouldUseDirectExecutable ? null : String(child.stderr || "").trim() || null,
      capture_path_exists: Boolean(capturePath && fs.existsSync(capturePath)),
      ...(openLaunchArtifacts || loadStartupArtifacts(attemptId)),
    };
    completedAttempt.died_before_js_startup = Boolean(
      completedAttempt.startup_status_observed && !completedAttempt.js_startup_observed,
    );
    completedAttempt.startup_boundary = deriveStartupBoundary(completedAttempt);
    completedAttempt.likely_failure_zone =
      completedAttempt.died_before_js_startup && completedAttempt.exit_signal === "SIGABRT"
        ? "APPKIT_LAUNCHSERVICES_PRE_JS"
        : completedAttempt.js_startup_observed && !completedAttempt.startup_renderer_loaded
          ? "ELECTRON_MAIN_PRE_RENDERER_LOAD"
          : completedAttempt.startup_renderer_loaded && !completedAttempt.startup_capture_written && completedAttempt.capture_requested
            ? "RENDERER_LOADED_PRE_CAPTURE"
            : null;
    completedAttempt.failure_class =
      completedAttempt.exit_signal === "SIGABRT" && completedAttempt.likely_failure_zone === "APPKIT_LAUNCHSERVICES_PRE_JS"
        ? "MACOS_STARTUP_ABORT_PRE_JS"
        : completedAttempt.launch_mode === "OPEN_BUNDLE_AUTOMATION" &&
            completedAttempt.startup_status_observed &&
            completedAttempt.js_startup_observed !== true &&
            (completedAttempt.exit_status ?? 0) !== 0
          ? "WRAPPER_OPEN_FAILURE"
          : completedAttempt.launch_mode === "OPEN_BUNDLE_AUTOMATION" &&
              !completedAttempt.startup_status_observed &&
              !completedAttempt.js_startup_observed
            ? "OPEN_LAUNCH_NO_STARTUP_EVIDENCE"
          : null;
    completedAttempt.classification =
      completedAttempt.failure_class ||
      ((completedAttempt.exit_status ?? 0) === 0 && !completedAttempt.exit_signal ? "READY_PATH_OK" : null);
    completedAttempt.operator_message =
      completedAttempt.failure_class === "MACOS_STARTUP_ABORT_PRE_JS"
        ? "Packaged app aborted before Electron JS startup at the macOS LaunchServices/AppKit boundary."
        : completedAttempt.failure_class === "WRAPPER_OPEN_FAILURE"
          ? "Wrapper/open launch failed before packaged app startup could be verified."
          : completedAttempt.failure_class === "OPEN_LAUNCH_NO_STARTUP_EVIDENCE"
            ? "Bundle-open launch returned without any Electron startup marker, so packaged startup could not be verified."
          : null;
    writeJson(latestLaunchAttemptPath, completedAttempt);
    appendJsonl(launchAttemptLogPath, completedAttempt);
    maybeWriteStartupAbortArtifact(completedAttempt);
    writeJson(launchCooldownPath, completedAttempt);
    process.exit(child.status ?? 0);
  } finally {
    releaseLaunchLock();
  }
}

main();
