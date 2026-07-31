# Deployment API — Testing

## Run Tests

Run the repo quality gate — it drives tests through the correct `.venv`. **Never run `pytest` directly** (wrong venv,
and it bypasses the enforced coding-standard checks).

```bash
bash scripts/quality-gates.sh            # ship mode (autofix + check)
bash scripts/quality-gates.sh --no-fix   # diagnostic / check only
```

SSOT: `/codex/06-coding-standards/quality-gates.md`.

## Coverage Target

70%+ coverage. Integration tests marked with `@pytest.mark.integration`.
