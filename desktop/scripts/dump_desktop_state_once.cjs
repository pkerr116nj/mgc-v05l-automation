const { app } = require("electron");
const fs = require("fs");
const path = require("path");

async function main() {
  await app.whenReady();
  const runtime = require("../dist/main/runtime.js");
  const fullState = await runtime.getDesktopState({
    includeHeavyPayload: true,
  });
  const compactedState = runtime.compactDesktopStateForRenderer(fullState, {
    paperTradeLogVisibleRange: null,
  });
  const outputPath = process.env.MGC_DESKTOP_STATE_DUMP_PATH
    ? path.resolve(process.env.MGC_DESKTOP_STATE_DUMP_PATH)
    : path.resolve(process.cwd(), "outputs/operator_dashboard/runtime/desktop_state_dump.json");
  fs.mkdirSync(path.dirname(outputPath), { recursive: true });
  fs.writeFileSync(
    outputPath,
    JSON.stringify(
      {
        recorded_at: new Date().toISOString(),
        full_state: fullState,
        compacted_state: compactedState,
      },
      null,
      2,
    ),
  );
  await app.quit();
}

main().catch(async (error) => {
  const outputPath = process.env.MGC_DESKTOP_STATE_DUMP_PATH
    ? path.resolve(process.env.MGC_DESKTOP_STATE_DUMP_PATH)
    : path.resolve(process.cwd(), "outputs/operator_dashboard/runtime/desktop_state_dump.error.json");
  fs.mkdirSync(path.dirname(outputPath), { recursive: true });
  fs.writeFileSync(
    outputPath,
    JSON.stringify(
      {
        recorded_at: new Date().toISOString(),
        error: String(error && error.stack ? error.stack : error),
      },
      null,
      2,
    ),
  );
  try {
    await app.quit();
  } catch {}
  process.exitCode = 1;
});
