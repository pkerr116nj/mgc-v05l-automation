const fs = require("node:fs");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

const desktopRoot = path.resolve(__dirname, "..");
const releaseRoot = path.join(desktopRoot, "release", "local");
const targetApp = path.join(releaseRoot, "MGC Operator.app");
const infoPlistPath = path.join(targetApp, "Contents", "Info.plist");
const resourcesAppDir = path.join(targetApp, "Contents", "Resources", "app");
function ensureLocalBundle() {
  if (fs.existsSync(targetApp) && fs.existsSync(infoPlistPath) && fs.existsSync(resourcesAppDir)) {
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

function pushArg(args, name, value) {
  if (value === undefined || value === null || value === "") {
    return;
  }
  args.push(`--${name}=${value}`);
}

function main() {
  ensureLocalBundle();
  const forwardedArgs = process.argv.slice(2);
  const openArgs = ["-W", "-n", targetApp, "--args", ...forwardedArgs];
  pushArg(openArgs, "mgc-capture-path", process.env.MGC_DESKTOP_CAPTURE_PATH);
  pushArg(openArgs, "mgc-capture-hash", process.env.MGC_DESKTOP_CAPTURE_HASH);
  pushArg(openArgs, "mgc-capture-delay-ms", process.env.MGC_DESKTOP_CAPTURE_DELAY_MS);
  pushArg(openArgs, "mgc-capture-and-exit", process.env.MGC_DESKTOP_CAPTURE_AND_EXIT);
  pushArg(openArgs, "mgc-capture-js", process.env.MGC_DESKTOP_CAPTURE_JS);
  pushArg(openArgs, "mgc-capture-scroll-section-title", process.env.MGC_DESKTOP_CAPTURE_SCROLL_SECTION_TITLE);
  pushArg(openArgs, "mgc-capture-scroll-row-text", process.env.MGC_DESKTOP_CAPTURE_SCROLL_ROW_TEXT);
  pushArg(openArgs, "mgc-capture-window-width", process.env.MGC_DESKTOP_CAPTURE_WINDOW_WIDTH);
  pushArg(openArgs, "mgc-capture-window-height", process.env.MGC_DESKTOP_CAPTURE_WINDOW_HEIGHT);
  pushArg(openArgs, "mgc-renderer-url", process.env.MGC_RENDERER_URL || process.env.VITE_DEV_SERVER_URL);

  const child = spawnSync("open", openArgs, {
    cwd: desktopRoot,
    env: process.env,
    stdio: "inherit",
  });
  process.exit(child.status ?? 0);
}

main();
