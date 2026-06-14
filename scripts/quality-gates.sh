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
# Bumped 23→24 (ci_local_qg_parity 2026-06-10): the base-service.sh deep-import check was
# FALSE-PASSING on macOS (`grep -vP` is unsupported by BSD grep → collapsed to empty) while CI
# (GNU grep) correctly flagged it — the exact local-green/CI-red divergence that blocked the
# deployment-api staging promotion. The portability bug is now fixed (`grep -P` → `rg --pcre2`),
# so the +1 "Deep unified lib imports" violation is now counted LOCALLY too. The 9 offenders are
# PRE-EXISTING two-level `from unified_api_contracts.registry.<X> import` imports (data_status_service /
# path_combinatorics / config / client_treasury / data_status_hierarchical) — none introduced by the
# monitoring-dashboard work. RATCHET 24→23: re-export those registry symbols at the UAC one-level
# facade (`unified_api_contracts/registry/__init__.py`) + switch the 9 call sites to
# `from unified_api_contracts.registry import <X>` — tracked in ci_local_qg_parity_2026_06_08.md.
# MAX_DURATION 300→700: basedpyright takes ~480s on this host; timeout is infra not code quality.
# Ratcheted 25→24 on 2026-06-11 (codex_violations_ratchet_to_five_2026_06_10): the transient 25th
# class was manifest-import-alignment — strategy-service was still declared in PM workspace-manifest
# after the treasury NAV-rollup relocation dropped the last import; the PM manifest edge is now
# removed, so the measured honest count is back at 24 (QG_SLICE=lint-codex verified). Budgets only
# ratchet DOWN from here — the ≤5 ceiling drive continues under the plan above.
# Ratcheted 24→22 on 2026-06-11 (codex_violations_ratchet_to_five_2026_06_10 Phase-1 P2 + Phase-2b):
# (1) file-size class CLEARED — the last four >900-line files split into facade packages with
#     byte-identical route tables / public surfaces (routes/data_status 2,550 → 6-module package;
#     services/data_status_drilldown 2,586 → 6-module package; services/shard_detail 1,777 →
#     5-module package; routes/deployments 968 → 3-module package; every module ≤ 731 lines).
# (2) deep-import class CLEARED — all 8 two-level `unified_api_contracts.registry.<X>` call sites
#     flipped to the one-level facade (`from unified_api_contracts.registry import <X>`; facade
#     re-exports landed at UAC@c8287d3). This also satisfies + supersedes the plan's
#     "budget 23→24 revert" item (we land at 22 < 23).
# Honest measured count 22 (QG_SLICE=lint-codex verified 2026-06-11). Next classes to clear per the
# plan: function-size (deployment_state/data_analytics/deployment_manager + data_status mixins),
# os.getenv, Any-types, schema-provenance.
# 2026-06-12 (codex ratchet plan Phase 4): os.getenv + comment-false-positive + empty-fallback
# sites cleared across 17 files -> honest measured V=16. Ratcheted 22 -> 16.
# 2026-06-12 (codex ratchet plan Phase 3+4): wave-4b agent cleared 10 classes (schema-provenance
# CORRECT-LOCAL triage, os.getenv, Any-types, imports-in-fn, empty-fallbacks et al) -> honest
# measured V=6. Ratcheted 16 -> 6.
CODEX_MAX_VIOLATIONS=6

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

# STEP 5.11/5.12 protocol-symbol excludes: monitor_live.py + monitor_scheduled.py match ONLY on
# `CloudTarget` — the UAC canonical StrEnum (unified_api_contracts.canonical.crosscutting.cloud_target)
# used as the LIVE_CLUSTER_REGISTRY cloud axis. That is contract usage, not a raw GCS/BigQuery
# protocol call (the legit case the base-service.sh comment names). Documented in
# QUALITY_GATE_BYPASS_AUDIT.md § "STEP 5.11/5.12 protocol-symbol exceptions" (2026-06-12).
HARDCODED_PROTO_EXCLUDE_GLOBS=(
    "--glob=!**/routes/monitor_live.py"
    "--glob=!**/routes/monitor_scheduled.py"
)
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
