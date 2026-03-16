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
MAX_DURATION=300

# Lazy / circular-import-avoidance imports: deployment-api uses deferred imports
# pervasively to break circular dependency chains between routes ↔ services ↔ workers
# and to gate mock-state loading behind CLOUD_MOCK_MODE. This is a deliberate
# architectural choice for this large service (~80 modules).
IMPORT_INSIDE_EXCLUDE_GLOBS=(
    "!**/deployment_api/**"
)

# File/function size exclusions:
# - test files: comprehensive integration suites that cover deployment processor +
#   data status + sync service; splitting would fragment test fixtures.
# - deployment_processor.py: orchestration module managing VM lifecycle across
#   multiple states — splitting would fragment state machine coherence.
# - cloud_builds.py: Cloud Build API adapter with typed dicts for 10 response shapes.
# - path_combinatorics.py: combinatoric path generation for multi-service data
#   status queries; each method handles a distinct dimension of the query matrix.
# - deployment_state.py routes: multi-stage status refresh that queries multiple
#   backends; splitting would create artificial state-passing overhead.
# - state_manager.py: lock acquisition + orphan cleanup + TTL management —
#   tightly coupled state-machine operations.
# - data_query_service.py: venue/instrument data query methods with complex
#   path resolution that must stay co-located.
# - event_processor.py: VM event + orphan cleanup processing pipelines.
# - deployment_manager.py: quota calculation + deployment creation + background
#   execution — tightly coupled deployment lifecycle.
# - data_analytics_service.py: multi-service data status aggregation.
# - data_status_service.py: CLI runner + shard calculation + completeness validation.
# - sync_service.py: lock acquisition + deployment processing pipeline.
# - deployment_state service: deployment status aggregation.
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

# RBAC schema deep imports: unified_internal_contracts.schemas.rbac is not
# re-exported from UIC top-level __init__.py yet. These 4 files need the deep
# import path until UIC adds rbac re-exports.
DEEP_IMPORT_EXCLUDE_GLOBS=(
    "!**/auth_middleware.py"
    "!**/rbac.py"
    "!**/services/user_management.py"
    "!**/routes/user_management.py"
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
