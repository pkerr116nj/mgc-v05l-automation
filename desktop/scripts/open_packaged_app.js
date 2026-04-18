const path = require("node:path");
const { spawnSync } = require("node:child_process");
const repoRoot = path.resolve(__dirname, "..", "..");
const operatorConsoleScript = path.join(repoRoot, "scripts", "run_supervised_paper_operator_console.sh");

const result = spawnSync("bash", [operatorConsoleScript, ...process.argv.slice(2)], {
  cwd: repoRoot,
  env: {
    ...process.env,
    CODEX_SANDBOX: "",
    CODEX_SHELL: "",
  },
  stdio: "inherit",
});
process.exit(result.status ?? 1);
