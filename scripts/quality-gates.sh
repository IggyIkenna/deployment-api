#!/usr/bin/env bash
# Repo-specific settings only. Body: unified-trading-pm/scripts/quality-gates-base/base-service.sh
SERVICE_NAME="deployment-api"
SOURCE_DIR="deployment_api"
MIN_COVERAGE=70
RUN_INTEGRATION=false
PYTEST_WORKERS=${PYTEST_WORKERS:-2}
LOCAL_DEPS=()
# asyncio.run() sync-bridge (async cache invalidation from sync context) — QUALITY_GATE_BYPASS_AUDIT.md §1.2
ASYNCIO_RUN_EXCLUDE_GLOBS=(
    "!**/workers/deployment_processor.py" "!**/services/deployment_state.py"
    "!**/services/event_processor.py" "!**/routes/deployment_state.py"
)
# Lazy imports to break circular deps across all routes/services/utils/workers — QUALITY_GATE_BYPASS_AUDIT.md §1.2
IMPORT_INSIDE_EXCLUDE_GLOBS=(
    "--glob" "!**/app_config.py" "--glob" "!**/lifespan.py"
    "--glob" "!**/health_routes.py" "--glob" "!**/main.py"
    "--glob" "!**/commentary/**" "--glob" "!**/routes/**"
    "--glob" "!**/services/**" "--glob" "!**/utils/**"
    "--glob" "!**/workers/**"
)
# Large infra files — infra complexity by design — QUALITY_GATE_BYPASS_AUDIT.md §1.3
FUNCTION_SIZE_EXTRA_EXCLUDES=(
    "!" "-path" "./deployment_api/workers/*" "!" "-path" "./deployment_api/routes/*"
    "!" "-path" "./deployment_api/utils/path_combinatorics.py"
    "!" "-path" "./deployment_api/services/*"
    "!" "-path" "./tests/unit/test_data_status_turbo.py"
    "!" "-path" "./tests/unit/test_sync_service.py"
    "!" "-path" "./tests/unit/test_deployment_processor.py"
    "!" "-path" "./tests/unit/test_auto_sync.py"
)
WORKSPACE_ROOT="$(cd "$(git rev-parse --show-toplevel)/.." && pwd)"
source "${WORKSPACE_ROOT}/unified-trading-pm/scripts/quality-gates-base/base-service.sh"

log_section "[5.X/6] UEI LIFECYCLE EVENT ENFORCEMENT (STARTED/STOPPED/FAILED)"
for event in STARTED STOPPED FAILED; do
    run_timeout 30 rg "log_event.*\"${event}\"" "${SOURCE_DIR}" --type py -q \
        || log_warn "Missing log_event('${event}') in ${SERVICE_NAME} — see codex 03-observability/lifecycle-events.md"
done
