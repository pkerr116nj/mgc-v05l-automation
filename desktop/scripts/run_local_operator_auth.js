const fs = require("node:fs");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

const desktopRoot = path.resolve(__dirname, "..");
const repoRoot = path.resolve(desktopRoot, "..");
const launchScriptPath = path.join(__dirname, "launch_local_app.js");
const packagedExecutablePath = path.join(
  repoRoot,
  "desktop",
  "release",
  "local",
  "MGC Operator.app",
  "Contents",
  "MacOS",
  "MGC Operator",
);
const localAuthStatePath = path.join(repoRoot, "outputs", "operator_dashboard", "local_operator_auth_state.json");

function readJson(pathname) {
  try {
    return JSON.parse(fs.readFileSync(pathname, "utf8"));
  } catch {
    return null;
  }
}

function listExistingAppProcesses() {
  const result = spawnSync("ps", ["-axo", "pid=,command="], {
    cwd: repoRoot,
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
      const match = line.match(/^(\d+)\s+(.*)$/);
      if (!match) {
        return null;
      }
      return {
        pid: Number(match[1]),
        command: match[2],
      };
    })
    .filter(Boolean)
    .filter((row) => row.pid !== process.pid)
    .filter((row) => row.command.includes("/MGC Operator.app/Contents/MacOS/MGC Operator"));
}

function authStateFingerprint(payload) {
  if (!payload || typeof payload !== "object") {
    return "";
  }
  return JSON.stringify({
    auth_session_active: Boolean(payload.auth_session_active),
    auth_session_expires_at: payload.auth_session_expires_at ?? null,
    last_auth_result: payload.last_auth_result ?? null,
    last_auth_detail: payload.last_auth_detail ?? null,
    updated_at: payload.updated_at ?? null,
  });
}

function sleepMs(ms) {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

function waitForAuthStateChange(previousFingerprint, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const currentState = readJson(localAuthStatePath);
    const currentFingerprint = authStateFingerprint(currentState);
    if (currentState && currentFingerprint && currentFingerprint !== previousFingerprint) {
      return currentState;
    }
    sleepMs(500);
  }
  return readJson(localAuthStatePath);
}

function printAuthStateSummary() {
  const authState = readJson(localAuthStatePath);
  if (!authState) {
    return;
  }
  console.log(
    JSON.stringify(
      {
        auth_available: Boolean(authState.auth_available),
        auth_method: authState.auth_method ?? null,
        auth_session_active: Boolean(authState.auth_session_active),
        auth_session_expires_at: authState.auth_session_expires_at ?? null,
        last_auth_result: authState.last_auth_result ?? null,
        last_auth_detail: authState.last_auth_detail ?? null,
        artifacts: authState.artifacts ?? null,
      },
      null,
      2,
    ),
  );
}

function main() {
  const reason =
    process.env.MGC_LOCAL_OPERATOR_AUTH_REASON || "Authenticate local operator access for this desktop session.";
  const authArgs = [
    "--mgc-authenticate-local-operator=1",
    `--mgc-local-operator-auth-reason=${reason}`,
    ...process.argv.slice(2),
  ];
  const existingProcesses = listExistingAppProcesses();
  const previousFingerprint = authStateFingerprint(readJson(localAuthStatePath));
  const result = existingProcesses.length
    ? spawnSync(packagedExecutablePath, authArgs, {
        cwd: repoRoot,
        env: {
          ...process.env,
          MGC_REPO_ROOT: process.env.MGC_REPO_ROOT || repoRoot,
        },
        stdio: "inherit",
      })
    : spawnSync(process.execPath, [launchScriptPath, ...authArgs], {
    cwd: repoRoot,
    env: {
      ...process.env,
      MGC_REPO_ROOT: process.env.MGC_REPO_ROOT || repoRoot,
    },
    stdio: "inherit",
  });

  waitForAuthStateChange(previousFingerprint, Number(process.env.MGC_LOCAL_OPERATOR_AUTH_TIMEOUT_MS || 120000));
  printAuthStateSummary();
  process.exit(result.status ?? 1);
}

main();
