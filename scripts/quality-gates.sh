#!/usr/bin/env bash
#
# Quality Gates for deployment-api
#
# Usage:
#   ./scripts/quality-gates.sh           # Run all checks (with auto-fix)
#   ./scripts/quality-gates.sh --lint    # Linting only (with auto-fix)
#   ./scripts/quality-gates.sh --test    # Tests only
#   ./scripts/quality-gates.sh --quick   # Unit tests only (fast)
#   ./scripts/quality-gates.sh --no-fix  # Skip auto-fix (CI mode)
#
set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
REPO_ROOT="$(dirname "$PROJECT_ROOT")"
cd "$PROJECT_ROOT"

# Ensure venv in non-CI
if [ -z "${GITHUB_ACTIONS:-}" ] && [ -z "${CLOUD_BUILD:-}" ]; then
    unset VIRTUAL_ENV
    command -v uv &>/dev/null || pip install uv --quiet
    [ ! -d ".venv" ] && uv venv .venv
    [ -f ".venv/bin/activate" ] && source .venv/bin/activate
    for lib in unified-cloud-interface unified-events-interface; do
        p="$REPO_ROOT/$lib"
        [ -d "$p" ] && [ -f "$p/pyproject.toml" ] && uv pip install -e "$p" --quiet 2>/dev/null || true
    done
    uv pip install -e ".[dev]" --quiet 2>/dev/null || true
fi

PYTHON_CMD="${PROJECT_ROOT}/.venv/bin/python"
[ ! -f "$PYTHON_CMD" ] && PYTHON_CMD="python3"

SOURCE_DIRS="deployment_api/ tests/"
RUN_LINT=true
RUN_TESTS=true
QUICK_MODE=false
AUTO_FIX=true

for arg in "$@"; do
    case $arg in
        --lint) RUN_LINT=true; RUN_TESTS=false ;;
        --test) RUN_LINT=false; RUN_TESTS=true ;;
        --quick) QUICK_MODE=true ;;
        --no-fix) AUTO_FIX=false ;;
        --fix) AUTO_FIX=true ;;
    esac
done

LINT_STATUS=0
TEST_STATUS=0

echo -e "${BLUE}DEPLOYMENT-API QUALITY GATES${NC}"
echo ""

if [ "$RUN_LINT" = true ] && [ "$AUTO_FIX" = true ]; then
    echo -e "${BLUE}[1] AUTO-FIX${NC}"
    ruff format $SOURCE_DIRS
    ruff check --fix $SOURCE_DIRS
fi

if [ "$RUN_LINT" = true ]; then
    echo -e "${BLUE}[2] LINTING${NC}"
    ruff check $SOURCE_DIRS || LINT_STATUS=1
fi

if [ "$RUN_TESTS" = true ]; then
    echo -e "${BLUE}[3] TESTS${NC}"
    if [ "$QUICK_MODE" = true ]; then
        $PYTHON_CMD -m pytest tests/ -v --tb=short -q || TEST_STATUS=1
    else
        $PYTHON_CMD -m pytest tests/ -v --tb=short || TEST_STATUS=1
    fi
fi

echo ""
[ $LINT_STATUS -eq 0 ] && [ $TEST_STATUS -eq 0 ] && echo -e "${GREEN}ALL QUALITY GATES PASSED${NC}" || exit 1
