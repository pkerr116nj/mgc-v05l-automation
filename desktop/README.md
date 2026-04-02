# MGC Operator Desktop

This workspace is the first-pass Electron + React desktop shell for the trading operations console.

## Current shape

- Electron main process owns app lifecycle and the local Python dashboard process.
- React renderer provides the operator shell, navigation, and page surfaces.
- The renderer prefers the live local dashboard API at `/health` and `/api/dashboard`.
- If the local API is unavailable, the Electron bridge falls back to the latest persisted operator snapshots in `outputs/operator_dashboard/`.

## Scripts

- `npm run dev`
  Starts the Vite renderer, compiles Electron main/preload TypeScript, then launches Electron.
- `npm run dev:launch`
  Builds the current desktop workspace and launches Electron against the production renderer bundle.
- `npm run build`
  Builds Electron main/preload output into `dist/main` and the renderer into `dist/renderer`.
- `npm run dist`
  Runs a production build and packages the desktop app via `electron-builder`.
- `npm run package:local`
  Builds a no-network local `MGC Operator.app` bundle into `release/local/` using the installed Electron runtime.
- `npm run package:dir`
  Builds an unpacked local desktop bundle into `release/`.
- `npm run package:mac`
  Builds a local macOS `.app` bundle (unsigned/unnotarized) into `release/`.
- `npm run package:open`
  Opens the first packaged `MGC Operator.app` found under `release/`.

## Local runtime integration

The main process calls the existing repo scripts instead of replacing Python behavior:

- `scripts/run_operator_dashboard.sh --no-open-browser --verify-dashboard-api`
- `scripts/stop_operator_dashboard.sh`

Startup policy:

- Preferred dashboard host/port defaults to `127.0.0.1:8790`.
- Override with `MGC_OPERATOR_DASHBOARD_HOST` and `MGC_OPERATOR_DASHBOARD_PORT`.
- Controlled port fallback is disabled by default.
- Enable explicit fallback only by setting `MGC_OPERATOR_DASHBOARD_ALLOW_PORT_FALLBACK=1`.
- The desktop UI shows preferred URL, chosen URL/port, ownership (`attached` vs `started`), and the latest bind/start error.

Renderer runtime controls use the existing dashboard action surface:

- `POST /api/action/start-paper`
- `POST /api/action/stop-paper`
- `POST /api/action/paper-halt-entries`
- `POST /api/action/paper-resume-entries`
- `POST /api/action/auth-gate-check`

## Notes

- This scaffold keeps the UI API-oriented so it can later point to a remote hosted control plane.
- Local packaging is defined for unsigned local testing; notarization/signing is not part of this workspace yet.
- `npm run package:local` is the deterministic no-network bundle path for this workspace. `npm run package:mac` still depends on `electron-builder` being able to fetch any missing Electron packaging assets.
