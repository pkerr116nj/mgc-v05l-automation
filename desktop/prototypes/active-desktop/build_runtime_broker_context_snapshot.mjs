import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const prototypeRoot = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = process.env.OBSERVATORY_REPO_ROOT
  ? path.resolve(process.env.OBSERVATORY_REPO_ROOT)
  : path.resolve(prototypeRoot, "../../..");
const inputPath = process.env.OBSERVATORY_RUNTIME_BROKER_INPUT_PATH
  ? path.resolve(process.env.OBSERVATORY_RUNTIME_BROKER_INPUT_PATH)
  : path.join(repoRoot, "outputs/track_b_execution_core/observatory/runtime_broker_context/latest_runtime_broker_context.json");
const outputPath = process.env.OBSERVATORY_RUNTIME_BROKER_OUTPUT_PATH
  ? path.resolve(process.env.OBSERVATORY_RUNTIME_BROKER_OUTPUT_PATH)
  : path.join(prototypeRoot, "runtime_broker_context_snapshot.generated.mjs");

const snapshot = JSON.parse(fs.readFileSync(inputPath, "utf8"));
if (snapshot.schema_version !== "observatory_runtime_broker_context_v1") {
  throw new Error("unsupported runtime/broker context schema");
}
fs.writeFileSync(outputPath, `export const runtimeBrokerContextSnapshot = ${JSON.stringify(snapshot, null, 2)};\n`, "utf8");
console.log(JSON.stringify({ ok: true, output: path.relative(repoRoot, outputPath), status: snapshot.status }, null, 2));
