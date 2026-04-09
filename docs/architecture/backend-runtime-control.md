# Backend Runtime Control

## Purpose

This document freezes the control-thread runtime procedure for local backend verification before other workstreams begin feature debugging.

The goal is to prevent false failures caused by:

- stale backend processes
- old working directories
- mismatched source trees
- checking endpoints against the wrong runtime instance

## Canonical Backend Entry

The canonical local backend entrypoint is:

- `python -c "from quant_platform.webapi.main import run; run()"`

The runtime wrapper is:

- [backend_control.ps1](/C:/Users/1/Desktop/AI/quantities_economy/scripts/dev/backend_control.ps1)

`quant_platform.webapi.main.run()` now reads:

- `QUANT_PLATFORM_WEB_HOST`
- `QUANT_PLATFORM_WEB_PORT`

This keeps the Python entry stable while allowing the control script to own runtime setup.

## Control Commands

Run from the repo root:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\dev\backend_control.ps1 -Action status
powershell -ExecutionPolicy Bypass -File .\scripts\dev\backend_control.ps1 -Action restart
powershell -ExecutionPolicy Bypass -File .\scripts\dev\backend_control.ps1 -Action smoke
```

Supported actions:

- `status`
- `start`
- `stop`
- `restart`
- `smoke`

## What The Control Script Guarantees

### Process cleanup

Before `start` or `restart`, the script:

- clears the previously recorded managed backend PID
- checks the target port for a listening process
- only force-stops listeners that match the managed backend command shape

This is intentionally narrower than "kill anything on port 8000".

### Stable launch context

The script launches the backend:

- from the current repo root
- with the repo `.venv` Python when available
- with output redirected to:
  - `backend-dev.out.log`
  - `backend-dev.err.log`

### Runtime state

The script writes state to:

- `.tmp/backend-runtime-state.json`

That file records:

- PID
- host
- port
- repo root
- log paths

## Fixed Smoke Checks

The canonical smoke probe is:

- [backend_smoke.py](/C:/Users/1/Desktop/AI/quantities_economy/scripts/dev/backend_smoke.py)

It verifies the live instance, not just the source tree.

### Required checks

1. `GET /health`
2. `GET /api/datasets`
3. `GET /api/datasets/request-options`
4. `GET /api/datasets/training`
5. `GET /api/datasets/{id}/dependencies`
6. `DELETE /api/datasets/{missing_id}`

### Expected behavior

- `request-options` returns `200`
- `training` returns `200`
- `dependencies` returns `200` for a dataset discovered from `/api/datasets`
- `DELETE` on a known-missing probe id returns source-consistent `404`

The delete probe is deliberately non-destructive. The control-thread goal is to verify that the live route exists and matches current source behavior before data-thread debugging begins.

## Wave-0 Gate

Wave 0 is considered passed only when a restarted runtime instance responds as expected to:

- `GET /api/datasets/request-options`
- `GET /api/datasets/training`
- `GET /api/datasets/{id}/dependencies`
- `DELETE /api/datasets/{id}`

and the responses are observed from the actively managed runtime, not from a stale process.

## Handoff Rule

Other threads should not diagnose missing backend functionality until:

1. the backend has been restarted through `backend_control.ps1`
2. `backend_control.ps1 -Action smoke` passes
3. the state file and process command line point to the current repo root
