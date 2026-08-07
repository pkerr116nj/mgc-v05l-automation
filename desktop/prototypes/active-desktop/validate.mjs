import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { scenarioNames, validateFixtures } from "./fixtures.mjs";

const prototypeRoot = path.dirname(fileURLToPath(import.meta.url));
const requiredFiles = ["index.html", "styles.css", "fixtures.mjs", "app.mjs", "build_market_tape_snapshot.mjs"];
const forbiddenTokens = [
  "runtime state",
  "strategy input",
  "submitOrder",
  "cancelOrder",
  "readFileSync(\"outputs",
  "fetch(\"/api",
  "requestAnimationFrame(draw",
];

const errors = [];
const fixtureResult = validateFixtures();
errors.push(...fixtureResult.errors);

for (const fileName of requiredFiles) {
  const filePath = path.join(prototypeRoot, fileName);
  if (!fs.existsSync(filePath)) {
    errors.push(`missing required file: ${fileName}`);
    continue;
  }
  const content = fs.readFileSync(filePath, "utf8");
  for (const token of forbiddenTokens) {
    if (content.includes(token)) errors.push(`${fileName} contains forbidden token: ${token}`);
  }
}

const adapter = fs.readFileSync(path.join(prototypeRoot, "build_market_tape_snapshot.mjs"), "utf8");
for (const token of ["submitOrder", "cancelOrder", "strategy input"]) {
  if (adapter.includes(token)) errors.push(`build_market_tape_snapshot.mjs contains forbidden authority token: ${token}`);
}
for (const token of ["observatory_market_tape_snapshot_v1", "PHASE1_COMPLETED_1M_CANDLE", "display_only", "broker_authority: false"]) {
  if (!adapter.includes(token)) errors.push(`build_market_tape_snapshot.mjs missing required contract token: ${token}`);
}
if (adapter.includes("broker_authority: true")) {
  errors.push("build_market_tape_snapshot.mjs grants broker authority");
}

const html = fs.readFileSync(path.join(prototypeRoot, "index.html"), "utf8");
for (const fileName of ["./styles.css", "./app.mjs"]) {
  if (!html.includes(fileName)) errors.push(`index.html does not reference ${fileName}`);
}

const css = fs.readFileSync(path.join(prototypeRoot, "styles.css"), "utf8");
for (const bannedPattern of [".card", ".kpi", "table", "status-tile"]) {
  if (css.includes(bannedPattern)) errors.push(`styles.css includes dashboard-like pattern: ${bannedPattern}`);
}

if (scenarioNames().length < 27) {
  errors.push(`expected at least 27 prototype scenarios, got ${scenarioNames().length}`);
}

if (errors.length) {
  console.error(JSON.stringify({ valid: false, errors }, null, 2));
  process.exit(1);
}

console.log(JSON.stringify({
  valid: true,
  scenarios: scenarioNames(),
  required_files: requiredFiles,
}, null, 2));
