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
# asyncio.run() in sync-bridge methods (calling async cache invalidation from sync context)
ASYNCIO_RUN_EXCLUDE_GLOBS=(
    "!**/workers/deployment_processor.py"
    "!**/services/deployment_state.py"
    "!**/services/event_processor.py"
    "!**/routes/deployment_state.py"
)
# Lazy imports throughout — large service uses deferred imports to break circular deps
IMPORT_INSIDE_EXCLUDE_GLOBS=(
    "--glob" "!**/app_config.py"
    "--glob" "!**/lifespan.py"
    "--glob" "!**/health_routes.py"
    "--glob" "!**/main.py"
    "--glob" "!**/commentary/pipeline_uat.py"
    "--glob" "!**/routes/cloud_builds.py"
    "--glob" "!**/routes/config.py"
    "--glob" "!**/routes/config_management.py"
    "--glob" "!**/routes/data_batch_processing.py"
    "--glob" "!**/routes/deployment_caching.py"
    "--glob" "!**/routes/deployment_state.py"
    "--glob" "!**/routes/deployment_validation.py"
    "--glob" "!**/routes/deployments.py"
    "--glob" "!**/routes/deployments_helpers.py"
    "--glob" "!**/routes/infra_health.py"
    "--glob" "!**/routes/service_status.py"
    "--glob" "!**/routes/service_status_cache.py"
    "--glob" "!**/routes/service_status_checkers.py"
    "--glob" "!**/routes/service_status_execution.py"
    "--glob" "!**/routes/service_status_fast_data.py"
    "--glob" "!**/routes/services.py"
    "--glob" "!**/routes/shard_management.py"
    "--glob" "!**/services/deployment_manager.py"
    "--glob" "!**/services/deployment_state.py"
    "--glob" "!**/services/event_processor.py"
    "--glob" "!**/services/state_manager.py"
    "--glob" "!**/services/sync_service.py"
    "--glob" "!**/utils/cache.py"
    "--glob" "!**/utils/cloud_storage_client.py"
    "--glob" "!**/utils/deployment_events.py"
    "--glob" "!**/utils/deployment_state_reader.py"
    "--glob" "!**/utils/path_combinatorics.py"
    "--glob" "!**/utils/storage_facade.py"
    "--glob" "!**/workers/auto_sync.py"
    "--glob" "!**/workers/deployment_processor.py"
)
# Large infra files — infra complexity by design
FUNCTION_SIZE_EXTRA_EXCLUDES=(
    "!" "-path" "./deployment_api/workers/deployment_processor.py"
    "!" "-path" "./deployment_api/workers/auto_sync.py"
    "!" "-path" "./deployment_api/routes/cloud_builds.py"
    "!" "-path" "./deployment_api/routes/batch_query_engine.py"
    "!" "-path" "./deployment_api/routes/data_batch_processing.py"
    "!" "-path" "./deployment_api/routes/service_status.py"
    "!" "-path" "./deployment_api/routes/service_status_execution.py"
    "!" "-path" "./deployment_api/routes/deployment_state.py"
    "!" "-path" "./deployment_api/utils/path_combinatorics.py"
    "!" "-path" "./deployment_api/services/state_manager.py"
    "!" "-path" "./deployment_api/services/data_query_service.py"
    "!" "-path" "./deployment_api/services/event_processor.py"
    "!" "-path" "./deployment_api/services/deployment_manager.py"
    "!" "-path" "./deployment_api/services/data_analytics_service.py"
    "!" "-path" "./deployment_api/services/data_status_service.py"
    "!" "-path" "./deployment_api/services/sync_service.py"
    "!" "-path" "./deployment_api/services/deployment_state.py"
    "!" "-path" "./tests/unit/test_data_status_turbo.py"
    "!" "-path" "./tests/unit/test_sync_service.py"
    "!" "-path" "./tests/unit/test_deployment_processor.py"
    "!" "-path" "./tests/unit/test_auto_sync.py"
)
WORKSPACE_ROOT="$(cd "$(git rev-parse --show-toplevel)/.." && pwd)"
source "${WORKSPACE_ROOT}/unified-trading-pm/scripts/quality-gates-base/base-service.sh"

# Codex enforcement: every entrypoint must emit STARTED, STOPPED, FAILED
# See: unified-trading-codex/03-observability/lifecycle-events.md § Lifecycle Event QG Enforcement
log_section "[5.X/6] UEI LIFECYCLE EVENT ENFORCEMENT (STARTED/STOPPED/FAILED)"
for event in STARTED STOPPED FAILED; do
    run_timeout 30 rg "log_event.*\"${event}\"" "${SOURCE_DIR}" --type py -q \
        || log_warn "Missing log_event('${event}') in ${SERVICE_NAME} — see codex 03-observability/lifecycle-events.md"
done
