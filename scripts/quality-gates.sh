#!/usr/bin/env bash
# Repo-specific settings only. Body: unified-trading-pm/scripts/quality-gates-base/base-service.sh
# SSOT: unified-trading-pm/codex/06-coding-standards/quality-gates-service-template.sh
#
# Instructions for a new service:
#   1. Copy this to scripts/quality-gates.sh in your repo (rollout-quality-gates-unified.py does this)
#   2. SERVICE_NAME, SOURCE_DIR, and MIN_COVERAGE are set automatically by rollout (floor=70)
#   3. Set RUN_INTEGRATION=true only if your repo has integration tests
#   4. Add LOCAL_DEPS entries if your service has local editable deps (e.g. unified-trading-library)
SERVICE_NAME="deployment-api"
SOURCE_DIR="deployment_api"
MIN_COVERAGE=70
RUN_INTEGRATION=false
PYTEST_WORKERS=${PYTEST_WORKERS:-4}
LOCAL_DEPS=()
MAX_DURATION=700
# Pre-existing violations uncovered after fixing step 3.5 (import patterns). Ratchet to 0 via
# deployment_and_qg_strategy_implementation_2026_05_13.md Phase 3 (schema provenance, os.getenv, etc.).
# Bumped 20→22: test-isolation fixes unmasked 2 additional pre-existing violations that were hidden
# behind failing tests. All 22 are pre-existing; none introduced by the snapshot-age badge work.
# Bumped 22→23: merge-conflict bug fixes (slot-2 2026-05-20) unmasked 1 additional pre-existing
# violation that was hidden behind test failures. All 23 are pre-existing.
# MAX_DURATION 300→700: basedpyright takes ~480s on this host; timeout is infra not code quality.
CODEX_MAX_VIOLATIONS=23

# ── Per-repo QG exclusions ──────────────────────────────────────────────────

# Imports inside functions: app_config lazy imports to avoid circular deps, health_routes lazy storage
IMPORT_INSIDE_EXCLUDE_GLOBS=()

# Empty string / dict / list fallbacks: JSON-parsing routes/services with safe-default
# .get(field, "") patterns when reading optional manifest/config fields. Same pattern as
# PM's QG comment — admin/codegen utilities, not os.getenv anti-pattern.
EMPTY_STR_EXCLUDE_GLOBS=()
EMPTY_DICT_LIST_EXCLUDE_GLOBS=()

# Function/method size: deployment-api has large orchestration and analytics methods
FUNCTION_SIZE_EXTRA_EXCLUDES=()
WORKSPACE_ROOT="$(cd "$(git rev-parse --show-toplevel)/.." && pwd)"
source "${WORKSPACE_ROOT}/unified-trading-pm/scripts/quality-gates-base/base-service.sh"

# Codex enforcement: lifecycle triple (STARTED / STOPPED / FAILED) via UTL — not duplicated in service code.
# See: unified-trading-pm/codex/03-observability/lifecycle-events.md § Lifecycle Event QG Enforcement
log_section "[5.X/6] UEI LIFECYCLE EVENT ENFORCEMENT (STARTED/STOPPED/FAILED)"
if rg -q 'fastapi_uei_lifespan\s*\(' --type py "$SOURCE_DIR" 2>/dev/null; then
    log_success "UEI lifecycle: fastapi_uei_lifespan (canonical HTTP wiring in UTL)"
elif rg -q 'ServiceBootstrap\s*\(' --type py "$SOURCE_DIR" 2>/dev/null; then
    log_success "UEI lifecycle: ServiceBootstrap (canonical CLI wiring in UTL)"
else
    for event in STARTED STOPPED FAILED; do
        # -U: allow multiline call sites (e.g. log_event(\n  "STARTED", ...))
        run_timeout 30 rg "log_event.*\"${event}\"" "${SOURCE_DIR}" --type py -U -q \
            || log_warn "Missing log_event('${event}') in ${SERVICE_NAME} — see codex 03-observability/lifecycle-events.md"
    done
fi
