# Local Startup Guide

This is the canonical startup document for the local web workbench.

If you change startup commands, ports, proxy targets, deployment entrypoints, environment-variable requirements, or the startup script behavior, update this file, `README.md`, and `scripts/dev/start_local.ps1` in the same PR.

## Supported Modes

There are two supported local modes:

1. Split dev mode: frontend and backend run separately for active development.
2. Single-port local deployment mode: build the frontend and let FastAPI serve `apps/web/dist`.

## Recommended One-Command Entry Point

Use the unified startup script from the repo root:

```powershell
powershell.exe -ExecutionPolicy Bypass -File .\scripts\dev\start_local.ps1 -Mode dev
```

or:

```powershell
powershell.exe -ExecutionPolicy Bypass -File .\scripts\dev\start_local.ps1 -Mode deploy
```

What it does:

- `dev` mode: restarts the backend on `8015` and starts the Vite frontend on `5173`
- `deploy` mode: builds `apps/web/dist`, restarts the backend on `8015`, and serves the built frontend from FastAPI

## Default Ports

- frontend dev: `http://127.0.0.1:5173`
- backend API: `http://127.0.0.1:8015`
- single-port local deployment: `http://127.0.0.1:8015`

Why `8015` for the backend:

- `apps/web/vite.config.ts` proxies `/api` to `http://127.0.0.1:8015` by default.
- The backend application code still defaults to port `8000` if no env var is set, so local startup should explicitly set `QUANT_PLATFORM_WEB_PORT=8015`.

## Prerequisites

Repository root: `C:\Users\1\Desktop\AI\quantities_economy`

Expected local tools:

- Python 3.11+
- Node.js + npm
- Windows note: if PowerShell script execution is restricted, use `npm.cmd` instead of `npm`

Prepare env:

1. Copy `.env.example` to `.env`
2. Fill `FRED_API_KEY` if you need real `macro/fred` ingestion
3. Restart backend after any `.env` change

## Split Dev Mode

Use this when developing frontend and backend together.

Preferred command:

```powershell
powershell.exe -ExecutionPolicy Bypass -File .\scripts\dev\start_local.ps1 -Mode dev
```

### Backend

PowerShell:

```powershell
$env:QUANT_PLATFORM_WEB_HOST = "127.0.0.1"
$env:QUANT_PLATFORM_WEB_PORT = "8015"
.venv\Scripts\python.exe -m quant_platform.webapi.main
```

Alternative helper script:

```powershell
powershell.exe -ExecutionPolicy Bypass -File .\scripts\dev\backend_control.ps1 -Action restart -Port 8015 -BindHost 127.0.0.1
```

Health check:

```powershell
Invoke-RestMethod http://127.0.0.1:8015/health
```

### Frontend

From `apps/web`:

```powershell
cmd /c npm.cmd install
cmd /c npm.cmd run dev -- --host 127.0.0.1 --port 5173
```

Open:

- frontend: `http://127.0.0.1:5173`
- backend API: `http://127.0.0.1:8015`

## Single-Port Local Deployment Mode

Use this when you want one local entrypoint and the backend should serve the frontend build directly.

Preferred command:

```powershell
powershell.exe -ExecutionPolicy Bypass -File .\scripts\dev\start_local.ps1 -Mode deploy
```

### Build frontend

From `apps/web`:

```powershell
cmd /c npm.cmd install
cmd /c npm.cmd run build
```

### Start backend

From repo root:

```powershell
$env:QUANT_PLATFORM_WEB_HOST = "127.0.0.1"
$env:QUANT_PLATFORM_WEB_PORT = "8015"
.venv\Scripts\python.exe -m quant_platform.webapi.main
```

Open:

- app: `http://127.0.0.1:8015`

How it works:

- FastAPI serves `apps/web/dist`
- `/assets/*` is mounted from the built frontend
- `/api/*` stays on the backend process

## Troubleshooting

### Frontend cannot reach backend

Check:

- backend is running on `127.0.0.1:8015`
- `apps/web/vite.config.ts` still proxies `/api` to `127.0.0.1:8015`
- you restarted the backend after changing `.env`

### `npm` fails in PowerShell with execution policy errors

Use:

```powershell
cmd /c npm.cmd run dev
```

or:

```powershell
cmd /c npm.cmd run build
```

### Backend starts on the wrong port

Set the env vars explicitly before startup:

```powershell
$env:QUANT_PLATFORM_WEB_HOST = "127.0.0.1"
$env:QUANT_PLATFORM_WEB_PORT = "8015"
```

## Change Management Rule

If your change touches any of the items below, you must update `README.md`, this file, and `scripts/dev/start_local.ps1` in the same PR:

- startup commands
- default ports
- frontend proxy target
- backend entrypoint
- static serving behavior
- required environment variables for startup
- startup script behavior
