# Cursor Rules Index

**Last Updated:** 2026-03-03
**Total Rules:** 106
**Always-Apply Rules:** 38

Rules are organized by category. Cursor loads recursively from subdirectories.

## Directory Structure

| Directory | Contents |
|-----------|----------|
| **core/** | alwaysApply rules — always active, easy to find |
| **quality-gates/** | Quality gates, basedpyright, ruff, limits |
| **testing/** | Test standards, coverage, integration layers |
| **ci-cd/** | CI/CD setup, quickmerge, Cloud Build |
| **architecture/** | DAG, batch-live, service structure |
| **imports/** | External imports, contracts, API usage |
| **config/** | ConfigStore, project IDs, workspace |
| **dependencies/** | Dep management, versions, uv |
| **documentation/** | Codex, plans, adding rules |
| **ui/** | UI setup, TypeScript quality gates |
| **services/** | Service setup, ServiceCLI |
| **standards/** | Coding standards, anti-patterns |
| **workflow/** | Agents, rollout, PR review |
| **misc/** | Observability, validators, sync |

---

## Always-Apply Rules (core/)

All rules in **core/** have `alwaysApply: true` — they are always active regardless of file context. See `core/*.mdc` for the full list.

---

## Priority Tier Guide

| Priority | Tier | Meaning |
|----------|------|---------|
| P100 | Blocking | No-summary-docs: violations block task completion |
| P95 | Structure | Plan-placement: controls where artifacts go |
| P90 | Safety | Runtime verification, never-revert, basedpyright-safety |
| P80 | Standards | Coding standards, import rules, type rules |
| P70 | Maintenance | Codex updates, rollout tracking, deprecation |
| P50 | Informational | Workflow guidance, sub-agent patterns |

---

## Context-Sensitive Rules (globs)

- `**/pyproject.toml`, `**/uv.lock` → uv-lock-file.mdc, workspace-venv-sync.mdc
- `**/.github/workflows/*.yml`, `**/pyproject.toml` → path-dependency-ci.mdc
- `**/*.py` → no-type-any-use-specific, no-empty-fallbacks, code-quality-limits
- `**/tests/**` → test-quality-standards, test-coverage-targets
- `unified-trading-codex/06-coding-standards/**` → coding-standards-alignment.mdc

---

## Related

- **Workspace Rules:** `../.cursorrules`
- **Codex:** `unified-trading-codex/06-coding-standards/cursor-rules-system.md`
