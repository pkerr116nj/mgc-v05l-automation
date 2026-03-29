const fs = require("node:fs");
const path = require("node:path");

const desktopRoot = path.resolve(__dirname, "..");
const sourceApp = path.join(desktopRoot, "node_modules", "electron", "dist", "Electron.app");
const releaseRoot = path.join(desktopRoot, "release", "local");
const targetApp = path.join(releaseRoot, "MGC Operator.app");
const appResourcesDir = path.join(targetApp, "Contents", "Resources", "app");
const sourcePackageJson = path.join(desktopRoot, "package.json");
const sourceDistDir = path.join(desktopRoot, "dist");

if (!fs.existsSync(sourceApp)) {
  console.error(`Electron.app was not found at ${sourceApp}. Run npm install first.`);
  process.exit(1);
}

if (!fs.existsSync(sourceDistDir)) {
  console.error(`Desktop dist/ was not found at ${sourceDistDir}. Run npm run build first.`);
  process.exit(1);
}

fs.mkdirSync(releaseRoot, { recursive: true });
fs.rmSync(targetApp, { recursive: true, force: true });
fs.cpSync(sourceApp, targetApp, { recursive: true });

fs.rmSync(appResourcesDir, { recursive: true, force: true });
fs.mkdirSync(appResourcesDir, { recursive: true });
fs.copyFileSync(sourcePackageJson, path.join(appResourcesDir, "package.json"));
fs.cpSync(sourceDistDir, path.join(appResourcesDir, "dist"), { recursive: true });

console.log(`Local app bundle created at ${targetApp}`);
