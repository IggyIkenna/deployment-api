#!/usr/bin/env bash
#
# Quality Gates Template — Python Service
# SSOT: unified-trading-codex/06-coding-standards/quality-gates-service-template.sh
#
# Canonical reference: instruments-service/scripts/quality-gates.sh
# Full CI/CD package:  unified-trading-codex/05-infrastructure/quickmerge-templates/service-with-deps/
#
# Instructions for a new service:
#   1. Copy this to scripts/quality-gates.sh in your repo
#   2. Set SERVICE_NAME, SOURCE_DIR, LOCAL_DEPS below
#   3. Run once; add bypass exceptions to QUALITY_GATE_BYPASS_AUDIT.md (not inline here)
#   4. Goal: pass with zero bypasses after full library adoption
#
# Requirements (must be in pyproject.toml [project.optional-dependencies] dev):
#   ruff==0.15.0, basedpyright, pytest, pytest-cov, pytest-xdist, pytest-timeout,
#   pip-audit, bandit
#
# Usage:
#   ./scripts/quality-gates.sh                 # Auto-fix then verify
#   ./scripts/quality-gates.sh --no-fix        # Verify only (CI mode)
#   ./scripts/quality-gates.sh --quick         # Unit tests only
#   ./scripts/quality-gates.sh --lint          # Lint only
#   ./scripts/quality-gates.sh --test          # Tests only
#   ./scripts/quality-gates.sh --skip-typecheck # Skip basedpyright type checking
#
set -e

# ── REPO-SPECIFIC SETTINGS ────────────────────────────────────────────────────
SERVICE_NAME="deployment-api"          # e.g. instruments-service
SOURCE_DIR="deployment_api"            # e.g. instruments_service  (underscore form)
MIN_COVERAGE=70  # Template default — set to (actual coverage - 1%) after first test run. See test-coverage-targets.mdc
RUN_INTEGRATION=false              # Set true when integration tests are stable
PYTEST_WORKERS=${PYTEST_WORKERS:-2} # Default 2; override via env (cap to avoid OOM)

# Path dependencies to install locally (from sibling repos).
# List each lib your pyproject.toml references as a path dep.
LOCAL_DEPS=(
    # "unified-config-interface"
    # "unified-trading-library"
    # "unified-domain-client"
    # "unified-events-interface"
    # "unified-market-interface"
)
# ── END REPO-SPECIFIC ─────────────────────────────────────────────────────────

QG_START=$(date +%s)
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
log_section() { echo -e "\n${BLUE}$1${NC}"; echo "----------------------------------------------------------------------"; }
log_success() { echo -e "${GREEN}✅ $1${NC}"; }
log_fail()    { echo -e "${RED}❌ $1${NC}"; }
log_warn()    { echo -e "${YELLOW}⚠️  $1${NC}"; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
REPO_ROOT="${REPO_ROOT:-$(dirname "$PROJECT_ROOT")}"
cd "$PROJECT_ROOT"

# ── SIZE LIMITS (per coding standards) ────────────────────────────────────────
MAX_FILE_LINES=900; FILE_WARN_LINES=700
MAX_FUNCTION_LINES=100; MAX_CLASS_LINES=500; MAX_METHOD_LINES=50

# ── PORTABLE TIMEOUT ──────────────────────────────────────────────────────────
run_timeout() {
    local secs=$1; shift
    if command -v timeout &>/dev/null; then timeout "$secs" "$@"
    elif command -v gtimeout &>/dev/null; then gtimeout "$secs" "$@"
    else "$@"; fi
}

# ── MODE ──────────────────────────────────────────────────────────────────────
FIX_MODE=true; QUICK_MODE=false; RUN_LINT=true; RUN_TESTS=true; SKIP_TYPECHECK=false
for arg in "$@"; do
    case $arg in
        --no-fix) FIX_MODE=false ;;   --quick) QUICK_MODE=true ;;
        --lint) RUN_TESTS=false ;;    --test) RUN_LINT=false ;;
        --skip-tests) RUN_TESTS=false ;;
        --fix) FIX_MODE=true ;;       --skip-typecheck) SKIP_TYPECHECK=true ;;
    esac
done

# ── BOOTSTRAP (local only; CI has its own setup) ─────────────────────────────
if [ -z "${GITHUB_ACTIONS:-}" ] && [ -z "${CI:-}" ] && [ -z "${CLOUD_BUILD:-}" ]; then
    command -v uv &>/dev/null || pip install uv --quiet
    uv lock 2>/dev/null || :
    [ ! -d ".venv" ] && uv venv .venv
    [ -f ".venv/bin/activate" ] && source .venv/bin/activate || :
    for lib in "${LOCAL_DEPS[@]}"; do
        [ -d "${REPO_ROOT}/$lib" ] && uv pip install -e "${REPO_ROOT}/$lib" --quiet 2>/dev/null || :
    done
    uv pip install -e ".[dev]" --quiet 2>/dev/null || uv pip install -e . --quiet 2>/dev/null || :
fi
PYTHON_CMD=".venv/bin/python"; [ ! -f "$PYTHON_CMD" ] && PYTHON_CMD="python3"

# Git-aware: only check staged files when committing
STAGED=$(git diff --cached --name-only --diff-filter=ACMR 2>/dev/null | grep '\.py$' | tr '\n' ' ' || :)
SOURCE_DIRS="${STAGED:-$SOURCE_DIR/ tests/}"
[ -n "$STAGED" ] && log_warn "Git-aware mode: $(echo "$STAGED" | wc -w | tr -d ' ') staged files"

export CLOUD_MOCK_MODE="true"; export GCP_PROJECT_ID="test-project"

# ── [0] ENVIRONMENT ────────────────────────────────────────────────────────────
log_section "[0/6] ENVIRONMENT"
ACTUAL_PY=$($PYTHON_CMD --version 2>&1 | awk '{print $2}' | cut -d'.' -f1,2)
[[ "$ACTUAL_PY" != "3.13" ]] && { log_fail "Python 3.13 required, found $ACTUAL_PY"; exit 1; }; log_success "Python $ACTUAL_PY"
command -v rg &>/dev/null || { log_fail "ripgrep required: brew install ripgrep"; exit 1; }; log_success "ripgrep OK"
[ -f "pyproject.toml" ] && grep -q '>=3.13,<3.14' pyproject.toml || { log_fail "pyproject.toml: requires-python = '>=3.13,<3.14'"; exit 1; }; log_success "pyproject.toml OK"
[[ ! -f "uv.lock" ]] && log_warn "uv.lock missing" || log_success "uv.lock present"
RUFF_CMD=".venv/bin/ruff"; command -v "$RUFF_CMD" &>/dev/null || RUFF_CMD="ruff"
RUFF_VER=$($RUFF_CMD --version 2>&1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || echo "0")
[[ "$RUFF_VER" != "0.15.0" ]] && log_warn "ruff 0.15.0 expected, found $RUFF_VER" || log_success "ruff $RUFF_VER"

# ── [1] AUTO-FIX (prettier + ruff, 30s each) ──────────────────────────────────
# Prettier runs FIRST on non-Python files to prevent ruff/prettier conflict in pre-commit hooks.
# Without this, committing JSON/YAML/MD files causes "MM" status and hook stash conflicts.
# See: 06-coding-standards/quality-gates.md § Formatter Conflict Resolution
if [ "$RUN_LINT" = true ] && [ "$FIX_MODE" = true ]; then
    log_section "[1/6] AUTO-FIX"
    # Pre-format non-Python files with prettier to avoid pre-commit hook conflicts
    if command -v npx &>/dev/null; then
        npx prettier --write "**/*.{md,json,yaml,yml}" --ignore-path .gitignore 2>/dev/null \
            && log_success "Prettier: non-Python files formatted" \
            || log_warn "Prettier not available or no files to format (skipping)"
    else
        log_warn "npx not available — skipping prettier pre-format (commit may require re-staging)"
    fi
    run_timeout 30 $RUFF_CMD format $SOURCE_DIRS || exit 1
    run_timeout 30 $RUFF_CMD check --fix $SOURCE_DIRS || exit 1
    log_success "Auto-fix complete"
fi

# ── [2] LINT (ruff, 30s) ──────────────────────────────────────────────────────
if [ "$RUN_LINT" = true ]; then
    log_section "[2/6] LINT"
    run_timeout 30 $RUFF_CMD check $SOURCE_DIRS && log_success "Lint PASSED" || { log_fail "Lint FAILED"; exit 1; }
fi

# ── [3] TESTS (pytest, timeout, xdist, coverage) ──────────────────────────────
if [ "$RUN_TESTS" = true ]; then
    log_section "[3/6] TESTS"
    $PYTHON_CMD -c "import pytest_timeout" 2>/dev/null || { log_fail "pytest-timeout required: uv pip install pytest-timeout"; exit 1; }
    $PYTHON_CMD -c "import xdist" 2>/dev/null || { log_fail "pytest-xdist required: uv pip install pytest-xdist"; exit 1; }
    COV="--cov=$SOURCE_DIR --cov-report=term-missing --cov-report=xml:coverage.xml --cov-fail-under=$MIN_COVERAGE"
    PARGS="-n $PYTEST_WORKERS --timeout=60 -v --tb=short"
    if [ "$QUICK_MODE" = true ] || [ "$RUN_INTEGRATION" != "true" ]; then
        $PYTHON_CMD -m pytest tests/unit/ $PARGS $COV || exit 1
    else
        $PYTHON_CMD -m pytest tests/unit/ tests/integration/ $PARGS $COV || exit 1
    fi
    log_success "Tests PASSED"
    [ ! -f "tests/unit/test_event_logging.py" ] && { log_fail "Missing tests/unit/test_event_logging.py"; exit 1; }
    [ ! -f "tests/unit/test_config.py" ] && { log_fail "Missing tests/unit/test_config.py"; exit 1; }
    log_success "Required test files present"

    # No duplicate test files (test_*_extended.py, test_*_additional.py)
    DUP=$(find tests/ -name "test_*_extended.py" -o -name "test_*_additional.py" 2>/dev/null | head -5 || :)
    [[ -n "$DUP" ]] && { log_fail "Duplicate test files — expand existing files instead:"; echo "$DUP"; exit 1; }
    log_success "No duplicate test files"

    # @pytest.mark.skip must have a reason comment on the preceding line
    SKIP_NO_REASON=$(rg "@pytest\.mark\.skip" --type py tests/ -B 1 2>/dev/null \
        | grep -v "# reason:\|# noqa\|^--" | grep "@pytest\.mark\.skip" || :)
    [[ -n "$SKIP_NO_REASON" ]] && { log_fail "pytest.mark.skip without reason comment — add '# reason: ...' above"; echo "$SKIP_NO_REASON" | head -3; exit 1; }
    log_success "All pytest.mark.skip have reason comments"
fi

# ── [3.5] IMPORT PATTERN STANDARDS ───────────────────────────────────────────
log_section "[3.5/6] IMPORT PATTERNS"
IP="${REPO_ROOT}/unified-trading-pm/scripts/check-import-patterns.py"
[ ! -f "$IP" ] && IP="${REPO_ROOT}/.cursor/scripts/check-import-patterns.py"  # legacy fallback
if [ -f "$IP" ]; then
    # Bypass: add --exclude flags for files whitelisted in QUALITY_GATE_BYPASS_AUDIT.md §1.2
    $PYTHON_CMD "$IP" --verbose 2>/dev/null && log_success "Import patterns PASSED" || { log_fail "Import patterns FAILED"; exit 1; }
else
    log_warn "check-import-patterns.py not found (unified-trading-pm/scripts/)"
fi

# ── [3.6] NO SERVICE-AS-PACKAGE DEPS (services only) ───────────────────────────
# Importing another service as a package is a violation; interaction is via messaging only (topology DAG SSOT).
NSD="${REPO_ROOT}/unified-trading-pm/scripts/check-no-service-deps.py"
if [ -f "$NSD" ]; then
    $PYTHON_CMD "$NSD" 2>/dev/null && log_success "No service-as-package deps" || { log_fail "Service must not depend on another service repo (use messaging per topology)"; exit 1; }
fi

# ── [4] TYPE CHECK (basedpyright, 120s, zombie cleanup) ──────────────────────
log_section "[4/6] TYPE CHECK"
if [ "$SKIP_TYPECHECK" != "true" ]; then
    cleanup_zombie_pyright() {
        ps -eo pid,etime,command 2>/dev/null | grep -E 'basedpyright.*index\.js' | grep -v grep | \
        while read -r pid etime _; do
            hours=0; echo "$etime" | grep -q '-' && hours=$(($(echo "$etime" | cut -d'-' -f1) * 24))
            [ "$(echo "$etime" | tr ':' '\n' | wc -l)" -eq 3 ] && hours=$(echo "$etime" | cut -d':' -f1)
            [ "${hours:-0}" -ge 2 ] && log_warn "Killing zombie basedpyright PID $pid" && kill -9 "$pid" 2>/dev/null || :
        done
    }
    cleanup_zombie_pyright
    command -v basedpyright &>/dev/null || { log_fail "basedpyright required: uv pip install basedpyright"; exit 1; }
    # CRITICAL: always pass the source directory explicitly — never run basedpyright without a path
    # (basedpyright . analyzes the entire workspace and consumes gigabytes of RAM)
    if [ -z "$SOURCE_DIR" ] || [ "$SOURCE_DIR" = "REPLACE_ME" ]; then
        log_fail "SOURCE_DIR not set — cannot run basedpyright safely"; exit 1
    fi
    export BASEDPYRIGHT_CACHE_DIR="${TMPDIR:-/tmp}/basedpyright-cache/${SERVICE_NAME:-$(basename "$PWD")}"
    mkdir -p "$BASEDPYRIGHT_CACHE_DIR"
    PYRIGHT_OUT=$(run_timeout 120 basedpyright "$SOURCE_DIR/" 2>&1); PYRIGHT_EXIT=$?
    if [ "$PYRIGHT_EXIT" -ne 0 ]; then echo "$PYRIGHT_OUT"; log_fail "Type check FAILED/timeout"; exit 1; fi
    WARN_COUNT=$(echo "$PYRIGHT_OUT" | grep -c " warning:" || :)
    if [ "${WARN_COUNT:-0}" -gt 0 ]; then
        echo "$PYRIGHT_OUT"
        log_fail "Type check FAILED — $WARN_COUNT warning(s) (zero-warning policy: promote all rules to error in [tool.basedpyright])"; exit 1
    fi
    log_success "Type check PASSED (0 errors, 0 warnings)"
fi
[ "$SKIP_TYPECHECK" = "true" ] && echo -e "${YELLOW}⚠️  Type check SKIPPED (--skip-typecheck flag)${NC}"

# ── [5] CODEX COMPLIANCE ──────────────────────────────────────────────────────
# All checks are blocking unless excluded via QUALITY_GATE_BYPASS_AUDIT.md.
# Add inline --glob exclusions below only for bypasses documented in that file.
log_section "[5/6] CODEX COMPLIANCE"
V=0

rg "print\(" --type py --glob "!tests/**" --glob "!scripts/**" "$SOURCE_DIR/" 2>/dev/null \
    && { log_fail "print() — use log_event() from UEI"; ((V++)); } || log_success "No print()"

rg "os\.getenv|os\.environ" --type py --glob "!tests/**" --glob "!scripts/**" --glob "!**/config.py" "$SOURCE_DIR/" 2>/dev/null \
    && { log_fail "os.getenv()/os.environ — use UnifiedCloudConfig for config, get_secret_client() for secrets"; ((V++)); } || log_success "No os.getenv()/os.environ"

rg 'os\.getenv\s*\([^)]+,\s*""\s*\)' --type py --glob "!tests/**" "$SOURCE_DIR/" 2>/dev/null \
    && { log_fail "os.getenv empty fallback — fail fast"; ((V++)); } || log_success "No os.getenv empty fallback"

rg "datetime\.now\(\)|datetime\.utcnow\(\)" --type py --glob "!tests/**" "$SOURCE_DIR/" 2>/dev/null \
    && { log_fail "Naive datetime — use datetime.now(timezone.utc)"; ((V++)); } || log_success "No naive datetime"

rg "except:" --type py --glob "!tests/**" "$SOURCE_DIR/" 2>/dev/null \
    && { log_fail "Bare except — use specific exception"; ((V++)); } || log_success "No bare except"


# Bypass: add --glob exclusions for files in QUALITY_GATE_BYPASS_AUDIT.md §1.1
for f in $(rg "import requests" --type py --glob "!tests/**" --glob "!scripts/**" "$SOURCE_DIR/" -l 2>/dev/null || :); do
    grep -q "async def" "$f" && { log_fail "requests in async: $f — use aiohttp"; ((V++)); break; }
done; [[ ${V} -eq $(( V )) ]] && log_success "No requests in async" 2>/dev/null || :

for f in $(rg "asyncio\.run\(" --type py --glob "!tests/**" --glob "!scripts/**" "$SOURCE_DIR/" -l 2>/dev/null || :); do
    grep -q "for \|while " "$f" && { log_fail "asyncio.run() in loop: $f — use asyncio.gather()"; ((V++)); break; }
done

INSIDE=$(rg "^[[:space:]]+import |^[[:space:]]+from .* import" --type py --glob "!tests/**" --glob "!**/__init__.py" \
    "$SOURCE_DIR/" 2>/dev/null || :)
# Bypass: add --glob exclusions for files in QUALITY_GATE_BYPASS_AUDIT.md §1.2
[[ -n "$INSIDE" ]] && { log_fail "Imports inside functions — move to top"; echo "$INSIDE" | head -3; ((V++)); } || log_success "No imports inside functions"

ANY=$(rg ": Any|-> Any|\[Any\]" --type py --glob "!tests/**" "$SOURCE_DIR/" 2>/dev/null | grep -v "type: ignore" || :)
[[ -n "$ANY" ]] && { log_fail "Any types (including dict[str, Any]) — use Pydantic models or specific types"; echo "$ANY" | head -3; ((V++)); } || log_success "No Any types"

# Untyped API responses — response.json() must go through model_validate(), not raw dict access
RAW_JSON=$(rg 'response\.json\(\)|await response\.json\(\)' --type py --glob "!tests/**" "$SOURCE_DIR/" 2>/dev/null \
    | grep -v 'model_validate\|cast(dict' || :)
[[ -n "$RAW_JSON" ]] && { log_fail "Raw response.json() — parse through Pydantic model_validate()"; echo "$RAW_JSON" | head -3; ((V++)); } || log_success "No raw response.json()"

rg '\.get\(["\x27][\w_]+["\x27]\s*,\s*["\x27]["\x27]\)' --type py --glob "!tests/**" "$SOURCE_DIR/" 2>/dev/null \
    && { log_fail "Empty string fallback — fail fast"; ((V++)); } || log_success "No empty string fallbacks"

ED=$(rg '\.get\s*\(\s*["\x27][^"\x27]+["\x27]\s*,\s*\{\}\s*\)' --type py --glob "!tests/**" "$SOURCE_DIR/" 2>/dev/null || :)
EL=$(rg '\.get\s*\(\s*["\x27][^"\x27]+["\x27]\s*,\s*\[\]\s*\)' --type py --glob "!tests/**" "$SOURCE_DIR/" 2>/dev/null || :)
[[ -n "$ED$EL" ]] && { log_fail "Empty dict/list fallback — fail fast"; ((V++)); } || log_success "No empty dict/list fallbacks"

rg "central-element-323112" tests/ 2>/dev/null \
    && { log_fail "Hardcoded prod project ID in tests — use 'test-project'"; ((V++)); } || log_success "No hardcoded project ID in tests"

rg "central-element-323112" --type py --glob "!tests/**" "$SOURCE_DIR/" 2>/dev/null \
    && { log_fail "Hardcoded project ID in production — use config.gcp_project_id"; ((V++)); } || log_success "No hardcoded project ID in production"

# GCP_PROJECT_ID is legacy — only GCP_PROJECT_ID is canonical
rg "GCP_PROJECT_ID" --type py --glob "!tests/**" --glob "!**/config.py" "$SOURCE_DIR/" 2>/dev/null \
    && { log_fail "Use GCP_PROJECT_ID not GCP_PROJECT_ID (except config.py backward compat)"; ((V++)); } || log_success "No GCP_PROJECT_ID usage"

# GCP auth: tests must use google.auth.default() — never pytest.skip for missing credential file
# Acceptable: pytest.skip inside _skip_integration_without_creds autouse fixture (integration marker pattern)
# Domain clients must come from unified_domain_client, not unified_trading_library
# Services should import: InstrumentsDomainClient, ExecutionDomainClient, create_*_client from UDS
UCS_DOMAIN=$(rg 'from unified_trading_library import[^#]*?(InstrumentsDomainClient|ExecutionDomainClient|MarketCandleDataDomainClient|MarketTickDataDomainClient|create_instruments_client|create_execution_client|create_features_client|create_market_candle_data_client|create_market_tick_data_client)' \
    --type py --glob "!tests/**" "$SOURCE_DIR/" 2>/dev/null || :)
[[ -n "$UCS_DOMAIN" ]] && { log_fail "Domain clients must come from unified_domain_client, not unified_trading_library"; echo "$UCS_DOMAIN" | head -5; ((V++)); } || log_success "Domain clients imported from unified_domain_client"

# No domain imports from UCS
DOMAIN_FROM_UCS=$(rg 'from unified_trading_library import.*(market_category|DomainValidation|UnifiedCloudServicesConfig)' \
    --type py "$SOURCE_DIR/" 2>/dev/null || :)
[[ -n "$DOMAIN_FROM_UCS" ]] && { log_fail "Service imports domain symbols from UCS — use unified_domain_client instead"; echo "$DOMAIN_FROM_UCS" | head -5; ((V++)); } || log_success "No domain imports from UCS"

# setup_events/setup_service uses sink= in production
SETUP_NO_SINK=$(rg 'setup_(events|service)\s*\(' --type py \
    --glob "!tests/**" "$SOURCE_DIR/" 2>/dev/null | grep -v 'sink=' || :)
[[ -n "$SETUP_NO_SINK" ]] && { log_fail "setup_events()/setup_service() called without sink= in production code"; echo "$SETUP_NO_SINK" | head -5; ((V++)); } || log_success "setup_service() uses sink= in all production call sites"

BAD_AUTH_SKIP=$(rg 'pytest\.skip.*[Cc]redential|pytest\.skip.*GOOGLE_APPLICATION_CREDENTIALS|if not.*gcp_credentials.*pytest\.skip\|if not.*cred_file.*pytest\.skip' \
    --type py tests/ 2>/dev/null \
    | grep -v "_skip_integration_without_creds\|No GCP credentials.*skipping integration\|No GCP credentials.*skipping Secret Manager\|Could not create/access" \
    || :)
[[ -n "$BAD_AUTH_SKIP" ]] && { log_fail "Tests skip due to missing credential file — use google.auth.default() + @pytest.mark.integration instead"; echo "$BAD_AUTH_SKIP" | head -5; ((V++)); } || log_success "No credential-file skip patterns in tests"

# GOOGLE_APPLICATION_CREDENTIALS must not appear in .env.example (we use ADC / GH token / Cloud SA)
[[ -f ".env.example" ]] && rg "GOOGLE_APPLICATION_CREDENTIALS" .env.example 2>/dev/null \
    && { log_fail ".env.example contains GOOGLE_APPLICATION_CREDENTIALS — remove it (use ADC, not SA key files)"; ((V++)); } || log_success "No GOOGLE_APPLICATION_CREDENTIALS in .env.example"

DI=$(rg 'from unified_[a-z_]+\.[a-zA-Z0-9_.]+\s+import' --type py --glob "!tests/**" --glob "!**/__init__.py" "$SOURCE_DIR/" 2>/dev/null || :)
[[ -n "$DI" ]] && { log_fail "Deep unified lib imports — use top-level"; echo "$DI" | head -3; ((V++)); } || log_success "No deep imports"

# Old event logging pattern — must use unified_events_interface directly
EL_OLD=$(rg "from unified_trading_library[. ].*(log_event|setup_events|setup_cloud_logging|observability)" --type py --glob "!tests/**" "$SOURCE_DIR/" 2>/dev/null || :)
[[ -n "$EL_OLD" ]] && { log_fail "Old event logging import — use 'from unified_events_interface import ...'"; echo "$EL_OLD" | head -3; ((V++)); } || log_success "Event logging imports from unified_events_interface"

# ============================================================
# STEP 5.5 — No direct cloud SDK imports (must route through UCLI/UCS)
# ============================================================
DIRECT_CLOUD=$(rg 'from google\.cloud import|^import boto3\b|^from boto3 import|^from botocore import' \
    --type py "${SOURCE_DIR}/" 2>/dev/null | grep -v __pycache__ | grep -v '\.venv' || :)
[[ -n "$DIRECT_CLOUD" ]] && {
    log_fail "Direct cloud SDK imports found (route through unified-cloud-interface instead):"
    echo "$DIRECT_CLOUD" | head -5
    ((V++))
} || log_success "No direct cloud SDK imports"

# ============================================================
# STEP 5.6 — Architecture Tier Compliance
# ============================================================
REPO_ARCH_TIER="${REPO_ARCH_TIER:-service}"
if [[ "$REPO_ARCH_TIER" == "0" ]]; then
    TIER_VIOLATIONS=$(rg 'from unified_trading_library|from unified_domain_client|from unified_trading_library' \
        --type py "${SOURCE_DIR}/" 2>/dev/null | grep -v __pycache__ || :)
    [[ -n "$TIER_VIOLATIONS" ]] && {
        log_fail "Tier 0 violation: imports from Tier 1+ library:"
        echo "$TIER_VIOLATIONS" | head -5
        ((V++))
    } || log_success "Tier 0 compliance: no Tier 1+ imports"
elif [[ "$REPO_ARCH_TIER" == "2" ]]; then
    TIER_VIOLATIONS=$(rg 'from unified_trading_library|from unified_trading_library' \
        --type py "${SOURCE_DIR}/" 2>/dev/null | grep -v __pycache__ || :)
    [[ -n "$TIER_VIOLATIONS" ]] && {
        log_fail "Tier 2 violation: imports from Tier 1 (unified-trading-library/unified-trading-library):"
        echo "$TIER_VIOLATIONS" | head -5
        ((V++))
    } || log_success "Tier 2 compliance: no Tier 1 imports"
else
    log_success "Tier compliance skipped (REPO_ARCH_TIER=$REPO_ARCH_TIER)"
fi

# pip install anywhere other than bootstrap (must use uv pip install)
PIP=$(rg "^RUN pip install|^RUN python -m pip" --glob "**/Dockerfile" --glob "**/*.sh" . 2>/dev/null \
    | grep -v "uv pip install" | grep -v "pip install uv" | grep -v "#" || :)
[[ -n "$PIP" ]] && { log_fail "Use 'uv pip install' not 'pip install'"; echo "$PIP" | head -3; ((V++)); } || log_success "No bare pip install"

BE=$(rg "except Exception:" --type py --glob "!tests/**" "$SOURCE_DIR/" 2>/dev/null || :)
# Bypass: add --glob exclusions for files in QUALITY_GATE_BYPASS_AUDIT.md §1.1
[[ -n "$BE" ]] && { log_warn "broad except Exception — document in QUALITY_GATE_BYPASS_AUDIT.md"; ((V++)); } || log_success "No broad except Exception"

# Swallowed errors — except that silently passes/returns None
SWALLOWED=$(rg "except Exception:" --type py --glob "!tests/**" "$SOURCE_DIR/" -A 2 2>/dev/null \
    | grep -E "^[[:space:]]+(pass|return None)$" || :)
[[ -n "$SWALLOWED" ]] && { log_fail "Swallowed errors — use @handle_api_errors or re-raise"; ((V++)); } || log_success "No swallowed errors"

# File size
SVIOL=""; SWARN=""
for f in $(find . -name "*.py" ! -path "./.venv/*" ! -path "./scripts/*" ! -path "./.git/*" 2>/dev/null); do
    lines=$(wc -l < "$f" 2>/dev/null || echo 0)
    [[ "$lines" -gt $MAX_FILE_LINES ]] && SVIOL="${SVIOL}\n  $f: $lines L"
    [[ "$lines" -gt $FILE_WARN_LINES && "$lines" -le $MAX_FILE_LINES ]] && SWARN="${SWARN}\n  $f: $lines L"
done
[[ -n "$SVIOL" ]] && { log_fail "Files exceed $MAX_FILE_LINES lines:$SVIOL"; ((V++)); } || log_success "File size OK"
[[ -n "$SWARN" ]] && log_warn "Approaching limit:$SWARN"

# Function/class/method size
FSIZES=""
for f in $(find . -name "*.py" ! -path "./.venv/*" ! -path "./scripts/*" ! -path "./.git/*" 2>/dev/null); do
    out=$($PYTHON_CMD -c "
import ast, sys
p=sys.argv[1]
try:
  with open(p,'r',encoding='utf-8') as fp: tree=ast.parse(fp.read())
  def v(n,par=None):
    if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)):
      l=(n.end_lineno or n.lineno)-n.lineno+1
      if isinstance(par,ast.ClassDef): l>$MAX_METHOD_LINES and print(f'  {p}:{n.lineno}:{par.name}.{n.name}(): {l}L')
      elif l>$MAX_FUNCTION_LINES: print(f'  {p}:{n.lineno}:{n.name}(): {l}L')
    elif isinstance(n,ast.ClassDef):
      l=(n.end_lineno or n.lineno)-n.lineno+1
      l>$MAX_CLASS_LINES and print(f'  {p}:{n.lineno}:{n.name}: {l}L')
    for c in ast.iter_child_nodes(n): v(c,n if isinstance(n,ast.ClassDef) else par)
  v(tree)
except: pass
" "$f" 2>/dev/null || :)
    [[ -n "$out" ]] && FSIZES="${FSIZES}\n${out}"
done
[[ -n "$FSIZES" ]] && { log_fail "Function/class/method size exceeded:$FSIZES"; ((V++)); } || log_success "Function/class/method size OK"

# Security: pip-audit (BLOCKING — OSV vulnerability database check)
if command -v pip-audit &>/dev/null; then
    pip-audit --format json -o /tmp/pip-audit-output.json 2>/dev/null \
        && log_success "pip-audit clean" \
        || { log_fail "pip-audit vulnerabilities found"; ((V++)); }
    # Store SBOM audit trail in GCS (non-blocking — upload failure does not fail the build)
    SERVICE_NAME="$SERVICE_NAME" python3 "$REPO_ROOT/unified-trading-pm/scripts/sbom-store.py" \
        /tmp/pip-audit-output.json 2>/dev/null || :
    # Internal advisory check (BLOCKING — checks unified-trading-pm/security/internal-advisories.yaml)
    if [[ -f "$REPO_ROOT/unified-trading-pm/scripts/check-internal-advisories.sh" ]]; then
        bash "$REPO_ROOT/unified-trading-pm/scripts/check-internal-advisories.sh" \
            && log_success "internal advisory check clean" \
            || { log_fail "internal advisory violation — see unified-trading-pm/security/internal-advisories.yaml"; ((V++)); }
    else
        log_warn "check-internal-advisories.sh not found at REPO_ROOT=$REPO_ROOT — skipping internal advisory check"
    fi
else
    log_fail "pip-audit required: uv pip install pip-audit"; ((V++))
fi

# Security: bandit
if command -v bandit &>/dev/null; then
    run_timeout 30 bandit -r "$SOURCE_DIR/" -ll 2>/dev/null && log_success "bandit clean" || { log_fail "bandit issues"; ((V++)); }
else
    log_fail "bandit required: uv pip install bandit"; ((V++))
fi

# Cloud Build validator
if [ -f "cloudbuild.yaml" ] && command -v gcloud &>/dev/null; then
    if git diff --name-only HEAD 2>/dev/null | grep -q "cloudbuild.yaml"; then
        gcloud meta validate-yaml cloudbuild.yaml 2>/dev/null && log_success "cloudbuild.yaml valid" || log_warn "cloudbuild.yaml validation failed"
    fi
fi

# CI/CD hygiene: ||true bypasses in quality gate scripts
BYPASS=$(rg "\|\|true|\|\| true" --glob "**/quality-gates.sh" --glob "**/quality-gates.yml" . 2>/dev/null \
    | grep -v "^#\|zombies\|pyright\|cleanup" || :)
[[ -n "$BYPASS" ]] && { log_fail "||true bypass in quality gates — fix the root cause"; echo "$BYPASS" | head -3; ((V++)); } || log_success "No ||true quality gate bypasses"

# ============================================================
# STEP 5.7 — No real cloud API calls in unit tests
# ============================================================
UNIT_CLOUD_CALLS=$(rg 'get_storage_client\(\)|get_secret_client\(\)|get_queue_client\(\)' \
    --type py tests/unit/ 2>/dev/null | grep -v '\.mock\.' | grep -v 'MagicMock' | grep -v 'patch' || :)
[[ -n "$UNIT_CLOUD_CALLS" ]] && {
    log_fail "Unit tests call real cloud APIs — use MagicMock(spec=StorageClient) instead"
    echo "$UNIT_CLOUD_CALLS" | head -5
    ((V++))
} || log_success "Unit tests appear cloud-agnostic"

# ============================================================
# STEP 5.8 — No backward-compatibility re-export stubs
# RULE: When moving a schema, update all consumers and delete the old file.
# Never leave a shim that re-exports from the new location.
# CODEX: cursor-rules/core/no-backward-compat-shims.mdc
# ============================================================
BACK_COMPAT=$(rg "# MIGRATED|backward compat|backward-compat|Re-export.*backward|re-export.*compat" \
    --type py --glob "!tests/**" --glob "!.venv*" "$SOURCE_DIR/" 2>/dev/null || :)
[[ -n "$BACK_COMPAT" ]] && {
    log_fail "Backward-compat pattern found — eliminate re-export stubs, aliases, and compat shims"
    log_fail "See: cursor-rules/core/no-backward-compat-shims.mdc"
    echo "$BACK_COMPAT" | head -5
    ((V++))
} || log_success "No backward-compat stubs"

# ============================================================
# STEP 5.9 — Schema placement compliance
# Domain data contracts (BaseModel/TypedDict/dataclass) must live in UIC domain/<service>/
# SchemaDefinition/ColumnSchema (parquet infra) may stay in schemas/output_schemas.py
# CODEX: 02-data/contracts-scope-and-layout.md, 02-data/schema-governance.md
# ============================================================
# Detect Pydantic BaseModel subclasses in service source outside of output_schemas.py
# (output_schemas.py is allowed to contain SchemaDefinition/ColumnSchema infra objects only)
DOMAIN_CONTRACTS_IN_SERVICE=$(rg 'class \w+\(BaseModel\)' --type py \
    --glob "!tests/**" --glob "!**/output_schemas.py" --glob "!**/__init__.py" \
    "$SOURCE_DIR/" 2>/dev/null | grep -v '#.*CORRECT-LOCAL' || :)
[[ -n "$DOMAIN_CONTRACTS_IN_SERVICE" ]] && {
    log_fail "Pydantic BaseModel subclasses found in service source — domain data contracts must live in UIC domain/<service-name>/"
    log_fail "See: unified-trading-pm/plans/active/SCHEMA_CONTRACTS_AUDIT.md Section 3b"
    echo "$DOMAIN_CONTRACTS_IN_SERVICE" | head -5
    ((V++))
} || log_success "No domain BaseModel contracts in service source"

# Detect TypedDict domain contracts in service source
TYPEDDICT_IN_SERVICE=$(rg 'class \w+\(TypedDict\)' --type py \
    --glob "!tests/**" --glob "!**/output_schemas.py" \
    "$SOURCE_DIR/" 2>/dev/null || :)
[[ -n "$TYPEDDICT_IN_SERVICE" ]] && {
    log_fail "TypedDict contracts found in service source — belong in UIC domain/<service-name>/"
    echo "$TYPEDDICT_IN_SERVICE" | head -3
    ((V++))
} || log_success "No TypedDict domain contracts in service source"

# ============================================================
# STEP 5.10 — Block direct cloud SDK imports outside UCI providers
# ============================================================
CLOUD_SDK_VIOLATIONS=$(rg "^from google\.cloud|^import boto3|^import botocore" \
    --type py \
    --glob '!.venv*' --glob '!**/.venv*/**' \
    --glob '!tests' \
    --glob '!unified_cloud_interface/providers/**' \
    -l . 2>/dev/null || :)
if [ -n "$CLOUD_SDK_VIOLATIONS" ]; then
    log_fail "STEP 5.10: Direct cloud SDK imports found. Use unified_cloud_interface instead:"
    echo "$CLOUD_SDK_VIOLATIONS"
    ((V++))
else
    log_success "STEP 5.10: No direct cloud SDK imports"
fi

# ============================================================
# STEP 5.11 — Block protocol-specific symbols in service code
# ============================================================
PROTOCOL_VIOLATIONS=$(rg "CloudTarget|upload_to_gcs_batch|gcs_bucket|bigquery_dataset|StandardizedDomainCloudService" \
    --type py \
    --glob '!.venv*' --glob '!**/.venv*/**' \
    --glob '!tests' \
    -l . 2>/dev/null || :)
if [ -n "$PROTOCOL_VIOLATIONS" ]; then
    log_fail "STEP 5.11: Protocol-specific symbols found. Use get_data_sink() / get_event_bus() from UCI instead:"
    echo "$PROTOCOL_VIOLATIONS"
    ((V++))
else
    log_success "STEP 5.11: No protocol-specific symbols in service code"
fi

# ============================================================
# STEP 5.12 — Services must not hardcode cloud protocol names
# ============================================================
echo "=== STEP 5.12: No hardcoded protocol names in service source ==="
HARDCODED_PROTO=$(rg \
  'gcs_bucket\s*=|bigquery_dataset\s*=|upload_to_gcs|CloudTarget\b|StandardizedDomainCloudService\b' \
  --type py \
  --glob '!.venv*' \
  --glob '!tests/**' \
  --glob '!scripts/**' \
  -l 2>/dev/null || :)
if [ -n "$HARDCODED_PROTO" ]; then
    log_fail "STEP 5.12: Hardcoded protocol/cloud names in service source (use get_data_sink/get_event_bus):"
    echo "$HARDCODED_PROTO"
    ((V++))
else
    log_success "STEP 5.12: No hardcoded protocol names"
fi

# STEP 5.13 — Schema placement advisory (cross-repo contract check)
# =====================================================================
echo "=== STEP 5.13: Schema placement advisory (cross-repo contract check) ==="
SCHEMA_DUPES=$(rg \
  'class\s+Canonical[A-Z]\w+\s*\(.*BaseModel' \
  --type py \
  --glob '!.venv*' \
  --glob '!tests/**' \
  --glob '!scripts/**' \
  "${SOURCE_DIR:-./}/" \
  2>/dev/null || :)
if [ -n "$SCHEMA_DUPES" ]; then
    log_warn "STEP 5.13: Pydantic classes matching 'Canonical*' pattern found in service source — verify these are NOT redefining UAC/UIC canonicals (canonical name collision causes type drift):"
    echo "$SCHEMA_DUPES" | head -10
    echo "(See: unified-trading-codex/02-data/schema-governance.md and cursor rule core/schema-governance-index.mdc)"
    # NOTE: Advisory only (log_warn, not log_fail). Some services may legitimately extend canonicals.
    # Manually verify: should import from unified_api_contracts or unified_internal_contracts instead.
else
    log_success "STEP 5.13: No canonical name collisions detected in service source"
fi

# STEP 5.21 — basedpyright config: all Any/Unknown rules must be "error" not "warning"
# Zero-warning policy requires rules to be errors so they block the QG at the config level too.
echo "=== STEP 5.21: basedpyright zero-warning policy (reportAny/reportUnknown* = \"error\") ==="
if [ -f "pyproject.toml" ]; then
    BP_VIOLATIONS=()
    for rule in reportAny reportUnknownVariableType reportUnknownParameterType reportUnknownMemberType reportUnknownArgumentType reportUnknownLambdaType; do
        if grep -qE "^\s*${rule}\s*=\s*[\"'](warning|none)[\"']" pyproject.toml 2>/dev/null; then
            BP_VIOLATIONS+=("$rule is set to warning/none — must be \"error\" or omitted")
        fi
    done
    if [ "${#BP_VIOLATIONS[@]}" -gt 0 ]; then
        for v in "${BP_VIOLATIONS[@]}"; do log_fail "STEP 5.21: $v"; done
        ((V++))
    else
        log_success "STEP 5.21: basedpyright Any/Unknown rules OK"
    fi
fi

[[ $V -gt 0 ]] && { log_fail "Codex compliance FAILED: $V violations"; exit 1; }
log_success "Codex compliance PASSED"

# ── [6] PRODUCTION READINESS (informational) ──────────────────────────────────
log_section "[6/6] PRODUCTION READINESS VALIDATORS"
VSCRIPT="${REPO_ROOT}/unified-trading-codex/scripts/run-all-validators.sh"
[ -f "$VSCRIPT" ] && "$VSCRIPT" --category all --failed-only 2>/dev/null || log_warn "Validators not available (optional)"

# ── DURATION CHECK (<2 min) ───────────────────────────────────────────────────
QG_END=$(date +%s); DUR=$((QG_END - QG_START))
[ $DUR -gt 120 ] && { log_fail "Quality gates must complete in <2 min (took ${DUR}s)"; exit 1; }
echo -e "\n${GREEN}======================================================================"
echo -e "✅ ALL QUALITY GATES PASSED (${DUR}s)${NC}"
