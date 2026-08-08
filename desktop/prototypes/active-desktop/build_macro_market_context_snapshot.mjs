import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const prototypeRoot = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = process.env.OBSERVATORY_REPO_ROOT
  ? path.resolve(process.env.OBSERVATORY_REPO_ROOT)
  : path.resolve(prototypeRoot, "../../..");
const inputPath = process.env.OBSERVATORY_MACRO_CONTEXT_INPUT_PATH
  ? path.resolve(process.env.OBSERVATORY_MACRO_CONTEXT_INPUT_PATH)
  : path.join(repoRoot, "outputs/track_b_execution_core/observatory/macro_market_context/latest_macro_market_context.json");
const outputPath = process.env.OBSERVATORY_MACRO_CONTEXT_OUTPUT_PATH
  ? path.resolve(process.env.OBSERVATORY_MACRO_CONTEXT_OUTPUT_PATH)
  : path.join(prototypeRoot, "macro_market_context_snapshot.generated.mjs");

const snapshot = JSON.parse(fs.readFileSync(inputPath, "utf8"));
if (snapshot.schema_version !== "observatory_macro_market_context_v1") {
  throw new Error("unsupported macro market context schema");
}
fs.writeFileSync(outputPath, `export const macroMarketContextSnapshot = ${JSON.stringify(snapshot, null, 2)};\n`, "utf8");
console.log(JSON.stringify({ ok: true, output: path.relative(repoRoot, outputPath), status: snapshot.status }, null, 2));
