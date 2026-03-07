# AGENTS.md

## Setup

```bash
uv sync --extra dev
source .venv/bin/activate
```

## Quality Gates

```bash
bash scripts/quality-gates.sh
```

## Type Checking

```bash
timeout 120 basedpyright deployment_api/
```

## Key Entry Points

- `deployment_api/` — FastAPI application
- `run-api.sh` — start the API server (uses gunicorn, see `gunicorn.conf.py`)

## Notes

- Initialize events with `from unified_events_interface import setup_events`
- Required env vars: `GCP_PROJECT_ID` — see `docs/`
- Requires GCP credentials: `gcloud auth application-default login`
- REST API for triggering and monitoring deployments
- Part of the 4-repo deployment cluster: `deployment-service` + `deployment-api` + `deployment-ui` + `system-integration-tests`
