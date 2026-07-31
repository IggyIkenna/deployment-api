# deployment-api

**deployment-api is the single deploy/launch + subscriptions backend for both the `deployment-ui` and the
`unified-trading-system-ui` (successor to the archived `user-management-ui`).** It is actively used and maintained — it
is NOT archived.

## What it does

- Backs the devops + launch consoles in `deployment-ui` (deploy/launch orchestration).
- Backs subscription/account management surfaces served through the trading-system UI.
- Runs as a Cloud Run service. See `docs/DEPLOYMENT_GUIDE.md`.

## Docs

- `docs/ARCHITECTURE.md` — service architecture.
- `docs/CONFIGURATION.md` — configuration + environment.
- `docs/DEPLOYMENT_GUIDE.md` — build + deploy.
- `docs/TESTING.md` — how to run the quality gate.
- `docs/SCHEMA_VALIDATION.md`, `docs/GCS_PATHS.md` — repo-specific reference stubs.

## Quality gate

```bash
bash scripts/quality-gates.sh
```

Never run `pytest` directly. See `/codex/06-coding-standards/quality-gates.md`.
