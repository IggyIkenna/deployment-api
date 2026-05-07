#!/usr/bin/env bash
# Repo-specific settings only. Body: unified-trading-pm/scripts/quality-gates-base/base-service.sh
# SSOT: unified-trading-codex/06-coding-standards/quality-gates-service-template.sh
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
PYTEST_WORKERS=${PYTEST_WORKERS:-2}
LOCAL_DEPS=()
MAX_DURATION=300

# Skip import pattern check: UTL deep imports (cloud_interface, config_interface, events_interface)
# not yet re-exported from unified_trading_library top-level __init__.py.
SKIP_IMPORT_PATTERNS=true

# ── Per-repo QG exclusions ──────────────────────────────────────────────────

# Imports inside functions: app_config lazy imports to avoid circular deps, health_routes lazy storage
IMPORT_INSIDE_EXCLUDE_GLOBS=(
    "!**/app_config.py"
    "!**/lifespan.py"
    "!**/health_routes.py"
    "!**/rbac.py"
    "!**/main.py"
    "!**/services/*.py"
    "!**/commentary/*.py"
    "!**/utils/*.py"
    "!**/routes/*.py"
    "!**/workers/*.py"
)

# Schema provenance: deployment-api-internal Pydantic models not yet in UAC
SCHEMA_PROVENANCE_SKIP=true

# Empty string / dict / list fallbacks: JSON-parsing routes/services with safe-default
# .get(field, "") patterns when reading optional manifest/config fields. Same pattern as
# PM's QG comment — admin/codegen utilities, not os.getenv anti-pattern.
EMPTY_STR_EXCLUDE_GLOBS=(
    "!**/services/user_management.py"
    "!**/services/data_status_drilldown.py"
    "!**/services/data_status_service.py"
    "!**/routes/epics.py"
    "!**/routes/checklist.py"
    "!**/routes/deployment_validation.py"
    "!**/routes/_code_builds_aws.py"
    "!**/routes/deployments.py"
    "!**/routes/builds.py"
)
EMPTY_DICT_LIST_EXCLUDE_GLOBS=(
    "!**/services/user_management.py"
    "!**/services/data_status_drilldown.py"
    "!**/services/data_status_service.py"
    "!**/routes/epics.py"
    "!**/routes/checklist.py"
    "!**/routes/deployment_validation.py"
    "!**/routes/_code_builds_aws.py"
    "!**/routes/deployments.py"
    "!**/routes/builds.py"
    "!**/services/upcoming_fixtures.py"
    "!**/routes/data_status.py"
    "!**/services/shard_detail.py"
)

# Direct cloud SDK: shard_detail.py uses google.cloud.storage for parquet footer reads
# (not covered by UCI's high-level abstraction layer — needed for byte-range fetches
# on options-chain bundles). Documented carve-out.
CLOUD_SDK_EXCLUDE_GLOBS=("!**/services/shard_detail.py")

# Deep unified lib imports: UTL sub-packages not re-exported from top-level
DEEP_IMPORT_EXCLUDE_GLOBS=(
    "!**/auth.py"
    "!**/auth_standardized.py"
    "!**/auth_middleware.py"
    "!**/rbac.py"
    "!**/main.py"
    "!**/__main__.py"
    "!**/health_routes.py"
    "!**/deployment_api_config.py"
    "!**/commentary/*.py"
    "!**/services/*.py"
    "!**/routes/*.py"
    "!**/workers/*.py"
    "!**/utils/*.py"
)

# Function/method size: deployment-api has large orchestration and analytics methods
FUNCTION_SIZE_EXTRA_EXCLUDES=(
    "!" "-path" "./${SOURCE_DIR}/utils/*"
    "!" "-path" "./${SOURCE_DIR}/routes/*"
    "!" "-path" "./${SOURCE_DIR}/services/*"
)

# pip-audit: ignore known CVEs (pending upgrade)
PIP_AUDIT_EXTRA_ARGS="--ignore-vuln CVE-2026-34073 --ignore-vuln CVE-2026-25645"

# Temporary rollout tolerance for known codex debt in this repo.
CODEX_MAX_VIOLATIONS=6
export CODEX_MAX_VIOLATIONS

WORKSPACE_ROOT="$(cd "$(git rev-parse --show-toplevel)/.." && pwd)"
source "${WORKSPACE_ROOT}/unified-trading-pm/scripts/quality-gates-base/base-service.sh"

# Codex enforcement: every entrypoint must emit STARTED, STOPPED, FAILED
# See: unified-trading-codex/03-observability/lifecycle-events.md § Lifecycle Event QG Enforcement
log_section "[5.X/6] UEI LIFECYCLE EVENT ENFORCEMENT (STARTED/STOPPED/FAILED)"
for event in STARTED STOPPED FAILED; do
    run_timeout 30 rg "log_event.*\"${event}\"" "${SOURCE_DIR}" --type py -q \
        || log_warn "Missing log_event('${event}') in ${SERVICE_NAME} — see codex 03-observability/lifecycle-events.md"
done
