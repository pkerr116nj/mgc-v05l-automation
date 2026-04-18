# MGC Operator Desktop

This workspace is the Electron + React operator console for the trading runtime.

## Current shape

- The service-first host owns the backend/operator service and supervised paper runtime.
- Electron is the optional operator console that attaches to that already-running host.
- React renderer provides the operator shell, navigation, and page surfaces.
- The renderer prefers the live local dashboard API at `/health` and `/api/dashboard`.
- If the local API is unavailable, the Electron bridge falls back to the latest persisted operator snapshots in `outputs/operator_dashboard/`.

## Scripts

- `npm run dev`
  Starts the Vite renderer, compiles Electron main/preload TypeScript, then launches Electron.
- `npm run dev:launch`
  Builds the local `MGC Operator.app` bundle and launches it through the service-first operator-console startup path.
- `npm run build`
  Builds Electron main/preload output into `dist/main` and the renderer into `dist/renderer`.
- `npm run start:bundle`
  Launches the packaged local `MGC Operator.app` through the service-first operator-console startup path.
- `npm run dist`
  Runs a production build and packages the desktop app via `electron-builder`.
- `npm run package:local`
  Builds a no-network local `MGC Operator.app` bundle into `release/local/` using the installed Electron runtime.
- `npm run package:dir`
  Builds an unpacked local desktop bundle into `release/`.
- `npm run package:mac`
  Builds a local macOS `.app` bundle (unsigned/unnotarized) into `release/`.
- `npm run package:open`
  Starts or verifies the service-first host and then launches the packaged operator console.

## Service-first runtime integration

Normal supervised-paper startup uses the existing repo scripts instead of letting Electron own the backend:

- `scripts/run_supervised_paper_host.sh`
- `scripts/show_headless_supervised_paper_status.sh`
- `scripts/run_supervised_paper_operator_console.sh`

Desktop startup policy:

- The service-first host is authoritative for supervised paper.
- Desktop launch first ensures the host is usable, then attaches the packaged console.
- Legacy desktop-managed backend launch remains diagnostic-only and is disabled by default.

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
