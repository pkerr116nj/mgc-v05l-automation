const fs = require("node:fs");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

const desktopRoot = path.resolve(__dirname, "..");
const repoRoot = path.resolve(desktopRoot, "..");
const releaseRoot = path.join(desktopRoot, "release", "local");
const localAppLink = path.join(releaseRoot, "MGC Operator.app");
const applicationsApp = "/Applications/MGC Operator.app";
const args = new Set(process.argv.slice(2));
const approved = args.has("--yes");
const skipBuild = args.has("--skip-build");

function run(command, commandArgs, options = {}) {
  const result = spawnSync(command, commandArgs, {
    cwd: options.cwd || desktopRoot,
    stdio: "inherit",
    env: process.env,
  });
  if ((result.status ?? 1) !== 0) {
    process.exit(result.status ?? 1);
  }
}

function resolveBundlePath(bundlePath) {
  const stat = fs.lstatSync(bundlePath);
  if (stat.isSymbolicLink()) {
    return path.resolve(path.dirname(bundlePath), fs.readlinkSync(bundlePath));
  }
  return bundlePath;
}

if (!approved) {
  console.error("Refusing to replace /Applications/MGC Operator.app without explicit --yes.");
  console.error("Run: npm run deploy:applications -- --yes");
  process.exit(2);
}

if (!skipBuild) {
  run("npm", ["run", "package:local"], { cwd: desktopRoot });
}

if (!fs.existsSync(localAppLink)) {
  console.error(`Local app bundle was not found at ${localAppLink}.`);
  process.exit(1);
}

const sourceApp = resolveBundlePath(localAppLink);
const metadataPath = path.join(sourceApp, "Contents", "Resources", "app", ".mgc-build-metadata.json");
let metadata = null;
try {
  metadata = JSON.parse(fs.readFileSync(metadataPath, "utf8"));
} catch (error) {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`Local app build metadata is missing or malformed at ${metadataPath}: ${message}`);
  process.exit(1);
}

fs.rmSync(applicationsApp, { recursive: true, force: true });
fs.cpSync(sourceApp, applicationsApp, { recursive: true, dereference: true });

console.log(`Copied ${sourceApp} -> ${applicationsApp}`);
console.log(`Build commit: ${metadata.git_commit || "UNKNOWN"}`);
console.log(`Build timestamp: ${metadata.build_generated_at || metadata.build_timestamp || "UNKNOWN"}`);
console.log("No broker, TWS, IBKR, Databento, or submit paths are invoked by this packaging script.");
console.log(`Repo: ${repoRoot}`);
