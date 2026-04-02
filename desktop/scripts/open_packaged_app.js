const fs = require("node:fs");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

const releaseRoot = path.resolve(__dirname, "..", "release");

function findPackagedApp(rootDir) {
  const pending = [rootDir];
  while (pending.length > 0) {
    const current = pending.pop();
    if (!fs.existsSync(current)) {
      continue;
    }
    const entries = fs.readdirSync(current, { withFileTypes: true });
    for (const entry of entries) {
      const fullPath = path.join(current, entry.name);
      if (entry.isDirectory() && entry.name === "MGC Operator.app") {
        return fullPath;
      }
      if (entry.isDirectory()) {
        pending.push(fullPath);
      }
    }
  }
  return null;
}

const packagedApp = findPackagedApp(releaseRoot);
if (!packagedApp) {
  console.error("Packaged app bundle not found under release/. Run `npm run package:mac` first.");
  process.exit(1);
}

const result = spawnSync("open", [packagedApp], { stdio: "inherit" });
process.exit(result.status ?? 0);
