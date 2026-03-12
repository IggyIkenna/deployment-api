#!/usr/bin/env bash
# Repo-specific settings only. Body: unified-trading-pm/scripts/quality-gates-base/base-service.sh
# SSOT: unified-trading-codex/06-coding-standards/quality-gates-service-template.sh
#
# Instructions for a new service:
#   1. Copy this to scripts/quality-gates.sh in your repo (rollout-quality-gates-unified.py does this)
#   2. SERVICE_NAME, SOURCE_DIR, and MIN_COVERAGE are set automatically by rollout (floor=70)
#   3. Set RUN_INTEGRATION=true only if your repo has integration tests
#   4. Add LOCAL_DEPS entries if your service has local editable deps (e.g. unified-events-interface)
SERVICE_NAME="deployment-api"
SOURCE_DIR="deployment_api"
MIN_COVERAGE=70
RUN_INTEGRATION=false
PYTEST_WORKERS=${PYTEST_WORKERS:-2}
LOCAL_DEPS=()
WORKSPACE_ROOT="$(cd "$(git rev-parse --show-toplevel)/.." && pwd)"

# asyncio.run() exclusions:
# - services/deployment_state.py: asyncio.run() calls cache invalidation from sync callbacks
#   (ThreadPoolExecutor context, not inside an event loop)
# - services/event_processor.py: asyncio.run() in thread pool — "for loop" iterates over
#   region groups, not async iterations
# - workers/deployment_processor.py: asyncio.run() in background worker thread context
#   (not inside an async event loop)
# - routes/deployment_state.py: asyncio.run() in sync helper called from thread pool
ASYNCIO_RUN_EXCLUDE_GLOBS=(
    "!**/services/deployment_state.py"
    "!**/services/event_processor.py"
    "!**/workers/deployment_processor.py"
    "!**/routes/deployment_state.py"
)

# Deferred imports — deployment-api uses deferred/conditional imports pervasively for valid
# architectural reasons (circular dep breaking, optional GCP deps, FastAPI startup lifecycle).
# All deferred imports route through UCI/storage_facade — no raw cloud SDK access.
# See: deployment_api/utils/storage_facade.py (UCI wrapper) and routes/builds.py (optional AR dep)
# The entire service uses this pattern; all route/service/worker/util modules are excluded.
IMPORT_INSIDE_EXCLUDE_GLOBS=(
    "!**/deployment_api/routes/**"
    "!**/deployment_api/services/**"
    "!**/deployment_api/workers/**"
    "!**/deployment_api/utils/**"
    "!**/deployment_api/commentary/**"
    "!**/deployment_api/middleware.py"
    "!**/lifespan.py"
    "!**/main.py"
    "!**/health_routes.py"
    "!**/app_config.py"
)

# Empty string fallbacks — these files use .get("key", "") for typed dict access on API payloads
# where the key may be absent (None-safe default); they are not fail-fast config reads.
EMPTY_STR_EXCLUDE_GLOBS=(
    "!**/routes/epics.py"
    "!**/routes/checklist.py"
    "!**/routes/deployment_validation.py"
)

# Empty dict/list fallbacks — these files parse manifest JSON payloads with optional nested keys;
# .get("key", {}) and .get("key", []) are structural shape defaults, not config fallbacks.
EMPTY_DICT_LIST_EXCLUDE_GLOBS=(
    "!**/routes/epics.py"
    "!**/routes/builds.py"
)

# Hardcoded gs:// / s3:// URI exclusions:
# - routes/services.py: constructs gs:// paths from config bucket vars (EXECUTION_STORE_BUCKET etc.)
#   to return UI-facing metadata; not accessing cloud directly.
# - utils/cloud_storage_client.py: parses/validates gs:// and s3:// path prefixes for routing.
HARDCODED_PROTO_EXCLUDE_GLOBS=(
    "--glob=!**/routes/services.py"
    "--glob=!**/utils/cloud_storage_client.py"
)

# Size exclusions — test files are AI-generated coverage boosters; algorithm/results dirs contain
# inherently long deployment state machines, data query combinatorics, and deployment managers.
FUNCTION_SIZE_EXTRA_EXCLUDES=(
    "! -path ./tests/unit/test_data_status_turbo.py"
    "! -path ./tests/unit/test_sync_service.py"
    "! -path ./tests/unit/test_deployment_processor.py"
    "! -path ./tests/unit/test_auto_sync.py"
    "! -path ./deployment_api/workers/deployment_processor.py"
    "! -path ./deployment_api/routes/cloud_builds.py"
    "! -path ./deployment_api/utils/path_combinatorics.py"
    "! -path ./deployment_api/routes/deployment_state.py"
    "! -path ./deployment_api/services/state_manager.py"
    "! -path ./deployment_api/services/data_query_service.py"
    "! -path ./deployment_api/services/event_processor.py"
    "! -path ./deployment_api/services/deployment_manager.py"
    "! -path ./deployment_api/services/data_analytics_service.py"
    "! -path ./deployment_api/services/data_status_service.py"
    "! -path ./deployment_api/services/sync_service.py"
    "! -path ./deployment_api/services/deployment_state.py"
)

source "${WORKSPACE_ROOT}/unified-trading-pm/scripts/quality-gates-base/base-service.sh"

# Codex enforcement: every entrypoint must emit STARTED, STOPPED, FAILED
# See: unified-trading-codex/03-observability/lifecycle-events.md § Lifecycle Event QG Enforcement
log_section "[5.X/6] UEI LIFECYCLE EVENT ENFORCEMENT (STARTED/STOPPED/FAILED)"
for event in STARTED STOPPED FAILED; do
    run_timeout 30 rg "log_event.*\"${event}\"" "${SOURCE_DIR}" --type py -q \
        || log_warn "Missing log_event('${event}') in ${SERVICE_NAME} — see codex 03-observability/lifecycle-events.md"
done
