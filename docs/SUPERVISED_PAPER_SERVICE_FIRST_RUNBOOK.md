# Supervised Paper Service-First Runbook

This runbook defines the preferred production-style operating model for the paper-only bridge.

## Preferred Launch Model

The backend/operator service and the supervised paper runtime are the real hosts.

Recommended production-style path:

1. Start the supervised paper host directly and keep it under a normal process supervisor:

   ```bash
   bash scripts/run_supervised_paper_host.sh
   ```

   This starts the shared paper runtime if needed, then runs the backend/operator service in the foreground.

2. Confirm the headless status contract from a second shell:

   ```bash
   bash scripts/show_headless_supervised_paper_status.sh
   ```

3. Optionally attach the desktop UI after the service host is already healthy.

The packaged desktop app is optional operator UI. It is not a prerequisite for runtime availability.

## Validation / Bootstrap Helper

For one-shot startup validation, use:

```bash
bash scripts/run_headless_supervised_paper_service.sh
```

This helper waits for the service-first path to reach the headless operability contract and then exits with a proof artifact. It is a bootstrap/verification helper, not the preferred long-lived host process.

## Headless Proof Artifacts

Authoritative headless artifacts:

- `outputs/operator_dashboard/runtime/headless_supervised_paper_status.json`
- `outputs/operator_dashboard/runtime/headless_supervised_paper_status.md`
- `outputs/operator_dashboard/runtime/headless_supervised_paper_service_startup.json`

Supporting backend artifacts:

- `outputs/operator_dashboard/startup_control_plane_snapshot.json`
- `outputs/operator_dashboard/supervised_paper_operability_snapshot.json`
- `outputs/operator_dashboard/runtime/operator_dashboard.json`

## What “Usable” Means

Headless supervised paper is usable only when:

- dashboard/backend is attached
- `/health` is ready
- startup control plane is `READY`
- supervised paper operability is `USABLE`
- paper runtime is running and ready
- packaged Electron launch is not required

## If The UI Will Not Launch

Do not restart the runtime just because the packaged app failed.

Instead:

1. Check headless status:

   ```bash
   bash scripts/show_headless_supervised_paper_status.sh
   ```

2. If `app_usable_for_supervised_paper` is `true`, keep the runtime running.
3. Treat packaged Electron failures as optional-console failures, not host failures.

## Stop Path

To stop the service host cleanly:

```bash
bash scripts/stop_headless_supervised_paper_service.sh
```

This stops:

- dashboard manager / backend service
- supervised paper runtime
