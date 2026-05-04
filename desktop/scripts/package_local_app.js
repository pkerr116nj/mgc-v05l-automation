const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

const desktopRoot = path.resolve(__dirname, "..");
const sourceApp = path.join(desktopRoot, "node_modules", "electron", "dist", "Electron.app");
const releaseRoot = path.join(desktopRoot, "release", "local");
const stagedReleaseRoot = path.join(os.tmpdir(), "mgc-operator-local");
const stagedTargetApp = path.join(stagedReleaseRoot, "MGC Operator.app");
const targetApp = path.join(releaseRoot, "MGC Operator.app");
const appResourcesDir = path.join(stagedTargetApp, "Contents", "Resources", "app");
const localConfigPath = path.join(appResourcesDir, ".mgc-local-config.json");
const buildMetadataPath = path.join(appResourcesDir, ".mgc-build-metadata.json");
const infoPlistPath = path.join(stagedTargetApp, "Contents", "Info.plist");
const sourceExecutable = path.join(stagedTargetApp, "Contents", "MacOS", "Electron");
const targetExecutable = path.join(stagedTargetApp, "Contents", "MacOS", "MGC Operator");
const LOCAL_BUNDLE_IDENTIFIER = "com.openai.mgcoperator.local";

function replacePlistString(plistText, key, value) {
  const pattern = new RegExp(`(<key>${key}<\\/key>\\s*<string>)([^<]*)(<\\/string>)`);
  return plistText.replace(pattern, `$1${value}$3`);
}

function rewriteBundleMetadata() {
  let plist = fs.readFileSync(infoPlistPath, "utf8");
  plist = replacePlistString(plist, "CFBundleDisplayName", "MGC Operator");
  plist = replacePlistString(plist, "CFBundleExecutable", "MGC Operator");
  plist = replacePlistString(plist, "CFBundleIdentifier", LOCAL_BUNDLE_IDENTIFIER);
  plist = replacePlistString(plist, "CFBundleName", "MGC Operator");
  fs.writeFileSync(infoPlistPath, plist, "utf8");

  if (!fs.existsSync(sourceExecutable)) {
    throw new Error(`Expected source executable at ${sourceExecutable}`);
  }
  fs.rmSync(targetExecutable, { force: true });
  fs.copyFileSync(sourceExecutable, targetExecutable);
  const mode = fs.statSync(targetExecutable).mode;
  fs.chmodSync(targetExecutable, mode);
}

function codesignBundle() {
  const result = spawnSync("codesign", ["--force", "--deep", "--sign", "-", stagedTargetApp], {
    cwd: desktopRoot,
    encoding: "utf8",
  });
  if ((result.status ?? 1) !== 0) {
    const stderr = String(result.stderr || "").trim();
    const stdout = String(result.stdout || "").trim();
    throw new Error(`codesign failed for ${stagedTargetApp}\n${stderr || stdout || "unknown codesign error"}`);
  }
}

function stripBundleMetadata() {
  const attrsToRemove = ["com.apple.FinderInfo", "com.apple.fileprovider.fpfs#P", "com.apple.provenance"];
  for (const attribute of attrsToRemove) {
    const result = spawnSync("xattr", ["-dr", attribute, stagedTargetApp], {
      cwd: desktopRoot,
      encoding: "utf8",
    });
    if ((result.status ?? 0) !== 0) {
      const stderr = String(result.stderr || "").trim();
      if (stderr && !stderr.includes("No such xattr")) {
        throw new Error(`xattr cleanup failed for ${attribute}\n${stderr}`);
      }
    }
  }
}

function publishReleaseSymlink() {
  fs.mkdirSync(releaseRoot, { recursive: true });
  fs.rmSync(targetApp, { recursive: true, force: true });
  fs.symlinkSync(stagedTargetApp, targetApp, "dir");
}
const sourcePackageJson = path.join(desktopRoot, "package.json");
const sourceDistDir = path.join(desktopRoot, "dist");

function commandOutput(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: options.cwd || path.resolve(desktopRoot, ".."),
    encoding: "utf8",
  });
  if ((result.status ?? 1) !== 0) {
    return null;
  }
  const output = String(result.stdout || "").trim();
  return output || null;
}

function buildMetadata() {
  const repoRoot = path.resolve(desktopRoot, "..");
  const commit = commandOutput("git", ["rev-parse", "--short=12", "HEAD"], { cwd: repoRoot });
  const commitFull = commandOutput("git", ["rev-parse", "HEAD"], { cwd: repoRoot });
  const branch = commandOutput("git", ["branch", "--show-current"], { cwd: repoRoot });
  const dirtyStatus = commandOutput("git", ["status", "--short"], { cwd: repoRoot });
  const generatedAt = new Date().toISOString();
  return {
    schema_version: "mgc_desktop_build_metadata_v1",
    app_name: "MGC Operator",
    build_generated_at: generatedAt,
    build_timestamp: generatedAt,
    git_commit: commit || "UNKNOWN",
    git_commit_full: commitFull || "UNKNOWN",
    git_branch: branch || "UNKNOWN",
    git_dirty: Boolean(dirtyStatus),
    repo_root: repoRoot,
    desktop_root: desktopRoot,
    packaged_app_path: targetApp,
    staged_app_path: stagedTargetApp,
    artifact_root: releaseRoot,
    packaging_mode: "local_workspace_bundle",
  };
}

function restoreRelativeSymlinks(sourceRoot, targetRoot) {
  const pending = [sourceRoot];
  while (pending.length > 0) {
    const currentSource = pending.pop();
    const currentTarget = path.join(targetRoot, path.relative(sourceRoot, currentSource));
    const entries = fs.readdirSync(currentSource, { withFileTypes: true });
    for (const entry of entries) {
      const sourceEntry = path.join(currentSource, entry.name);
      const targetEntry = path.join(currentTarget, entry.name);
      if (entry.isDirectory()) {
        pending.push(sourceEntry);
        continue;
      }
      if (!entry.isSymbolicLink()) {
        continue;
      }
      const linkValue = fs.readlinkSync(sourceEntry);
      fs.rmSync(targetEntry, { force: true, recursive: true });
      fs.symlinkSync(linkValue, targetEntry);
    }
  }
}

if (!fs.existsSync(sourceApp)) {
  console.error(`Electron.app was not found at ${sourceApp}. Run npm install first.`);
  process.exit(1);
}

if (!fs.existsSync(sourceDistDir)) {
  console.error(`Desktop dist/ was not found at ${sourceDistDir}. Run npm run build first.`);
  process.exit(1);
}

fs.mkdirSync(stagedReleaseRoot, { recursive: true });
fs.rmSync(stagedTargetApp, { recursive: true, force: true });
fs.cpSync(sourceApp, stagedTargetApp, { recursive: true, dereference: true });
restoreRelativeSymlinks(sourceApp, stagedTargetApp);

fs.rmSync(appResourcesDir, { recursive: true, force: true });
fs.mkdirSync(appResourcesDir, { recursive: true });
fs.copyFileSync(sourcePackageJson, path.join(appResourcesDir, "package.json"));
fs.cpSync(sourceDistDir, path.join(appResourcesDir, "dist"), { recursive: true });
const metadata = buildMetadata();
fs.writeFileSync(
  localConfigPath,
  `${JSON.stringify(
    {
      repo_root: path.resolve(desktopRoot, ".."),
      desktop_root: desktopRoot,
      generated_at: metadata.build_generated_at,
      packaging_mode: "local_workspace_bundle",
    },
    null,
    2,
  )}\n`,
  "utf8",
);
fs.writeFileSync(buildMetadataPath, `${JSON.stringify(metadata, null, 2)}\n`, "utf8");
rewriteBundleMetadata();
stripBundleMetadata();
codesignBundle();
publishReleaseSymlink();

console.log(`Local app bundle staged at ${stagedTargetApp}`);
console.log(`Local app bundle linked at ${targetApp}`);
console.log(`Local app build metadata written at ${buildMetadataPath}`);
console.log(`Local app build commit ${metadata.git_commit} generated at ${metadata.build_generated_at}`);
