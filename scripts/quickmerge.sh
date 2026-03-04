#!/bin/bash
# CANONICAL QUICKMERGE — unified-trading-system
#
# Single source of truth for all repos. Copy to scripts/quickmerge.sh.
#
# Usage:
#   ./scripts/quickmerge.sh "commit message"
#   ./scripts/quickmerge.sh "commit message" --files "path1 path2 path3"
#   ./scripts/quickmerge.sh "commit message" --dep-branch "my-feature"
#   ./scripts/quickmerge.sh "commit message" --quick
#   ./scripts/quickmerge.sh "commit message" --skip-tests
#   ./scripts/quickmerge.sh "commit message" --skip-typecheck
#
# Flags:
#   --files "p1 p2"    Stage only these paths (multi-agent: avoid committing other agents' work)
#   --dep-branch NAME  Branch isolation when dependencies have uncommitted changes
#   --quick            Skip only act simulation (Stage 4); all other checks run
#   --skip-tests       Pass --skip-tests to quality-gates.sh (lint+type+codex only)
#   --skip-typecheck   Pass --skip-typecheck to quality-gates.sh (skips basedpyright only)
#
# Pipeline:
#   1. Dependency validation (.dependency-matrix.json)
#   2. Pre-flight audit (always runs — never skipped)
#   3. Local quality gates (two-phase: auto-fix → verify)
#   4. Act simulation (default; skip with --quick)
#   5. Create PR + enable auto-merge
#
# Prerequisites:
#   - gh CLI installed and authenticated (gh auth login)
#   - Auto-merge enabled on the repo (Settings > General > Allow auto-merge)
#
# Notes:
#   - If quickmerge fails and you fix it: run quickmerge again. Do NOT
#     run quality gates first — quickmerge already runs quality gates.
#   - Agent sessions MUST use --files with the list of changed files to avoid
#     committing other agents' partial work.
#   - Script stays on PR branch after creating PR. To return to main after merge:
#     git checkout main && git pull

set -e

WORKSPACE_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

# ── PARSE ARGUMENTS ───────────────────────────────────────────────────────────
COMMIT_MSG="chore: automated update"
FILES_ARG=""
DEP_BRANCH=""
SKIP_TESTS=""
SKIP_TYPECHECK=""
QUICK=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --files)         FILES_ARG="$2";  shift 2 ;;
        --dep-branch)    DEP_BRANCH="$2"; shift 2 ;;
        --skip-tests)    SKIP_TESTS="--skip-tests"; shift ;;
        --skip-typecheck) SKIP_TYPECHECK="--skip-typecheck"; shift ;;
        --quick)         QUICK=true; shift ;;
        *)               COMMIT_MSG="$1"; shift ;;
    esac
done

REPO_DIR="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
REPO_NAME=$(basename "$REPO_DIR")
cd "$REPO_DIR"

# ── ACTIVATE VENV ─────────────────────────────────────────────────────────────
VENV_ACTIVATED=0
if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
    VENV_ACTIVATED=1
    echo "[$REPO_NAME] Using .venv (Python $(python --version 2>&1))"
elif [ -f ".venv/Scripts/activate" ]; then
    source .venv/Scripts/activate
    VENV_ACTIVATED=1
    echo "[$REPO_NAME] Using .venv (Python $(python --version 2>&1))"
else
    echo "[$REPO_NAME] ⚠️  No .venv found — using system Python"
fi

# ── INSTALL DEPS ──────────────────────────────────────────────────────────────
if [ -f "pyproject.toml" ]; then
    echo "[$REPO_NAME] Installing project dependencies..."
    command -v uv >/dev/null 2>&1 || pip install uv --quiet
    uv pip install -e ".[dev]" --quiet 2>/dev/null || uv pip install -e . --quiet 2>/dev/null || true
fi

# ── EARLY EXIT: nothing to commit ─────────────────────────────────────────────
git fetch origin main --quiet 2>/dev/null || true
if [ -z "$(git status --porcelain)" ] && git diff origin/main --quiet 2>/dev/null; then
    echo "[$REPO_NAME] Nothing to commit — exiting fast"
    exit 0
fi

# ============================================================================
# STAGE 1: DEPENDENCY VALIDATION
# ============================================================================
echo "=========================================="
echo "STAGE 1: Dependency Validation"
echo "=========================================="

if [ -f ".dependency-matrix.json" ]; then
    DEPS=$(jq -r '.dependencies[].name' .dependency-matrix.json 2>/dev/null || echo "")
    if [ -n "$DEPS" ]; then
        echo "Checking dependencies vs origin/main..."
        HAS_DIFF=false
        LAST_DEP_PATH=""
        for dep in $DEPS; do
            dep_path="$WORKSPACE_ROOT/$dep"
            if [ -d "$dep_path" ]; then
                cd "$dep_path"
                git fetch origin main --quiet 2>/dev/null || true
                if ! git diff origin/main --quiet 2>/dev/null; then
                    HAS_DIFF=true
                    LAST_DEP_PATH="$dep_path"
                    echo "❌ $dep: DIFFERS from main"
                else
                    echo "✅ $dep: Matches main"
                fi
                cd "$REPO_DIR"
            fi
        done

        if [ "$HAS_DIFF" = "true" ] && [ -z "$DEP_BRANCH" ]; then
            echo ""
            echo "═══════════════════════════════════════════════════════"
            echo "❌ DEPENDENCY CONFLICT DETECTED"
            echo "═══════════════════════════════════════════════════════"
            echo ""
            echo "Dependencies differ from main, but no --dep-branch specified."
            echo ""
            echo "Option 1: DISCARD local dependency changes"
            echo "  cd $LAST_DEP_PATH && git reset --hard origin/main"
            echo ""
            echo "Option 2: USE BRANCH ISOLATION (recommended)"
            echo "  bash scripts/quickmerge.sh \"$COMMIT_MSG\" --dep-branch \"my-feature\""
            echo ""
            echo "═══════════════════════════════════════════════════════"
            exit 1
        fi

        if [ -n "$DEP_BRANCH" ]; then
            echo "✅ --dep-branch specified: $DEP_BRANCH (branch isolation mode)"
        fi
    else
        echo "✅ No dependencies found in .dependency-matrix.json"
    fi
else
    echo "✅ No .dependency-matrix.json (no path dependencies)"
fi

echo ""

# ============================================================================
# STAGE 2: PRE-FLIGHT AUDIT (always runs — never skipped)
# ============================================================================
echo "=========================================="
echo "STAGE 2: Pre-flight Audit"
echo "=========================================="

if [ -f "$WORKSPACE_ROOT/.cursor/scripts/pre-flight-audit.sh" ]; then
    if bash "$WORKSPACE_ROOT/.cursor/scripts/pre-flight-audit.sh" "$REPO_NAME"; then
        echo "[$REPO_NAME] ✅ Pre-flight audit PASSED"
    else
        echo "[$REPO_NAME] ❌ Pre-flight audit FAILED"
        exit 1
    fi
else
    echo "[$REPO_NAME] ⚠️  pre-flight-audit.sh not found (skipping)"
fi

echo ""

# ── ENVIRONMENT AUTO-DETECT ───────────────────────────────────────────────────
if [ -f ".env" ]; then
    set -a
    grep -v '^#' .env | grep '=' | while IFS='=' read -r k _; do
        export "$k"
    done 2>/dev/null || true
    set +a
fi

CURRENT_BRANCH=$(git branch --show-current 2>/dev/null || echo "main")
if [ -z "${ENVIRONMENT:-}" ]; then
    if [ "$CURRENT_BRANCH" = "main" ] || [ "${PROD_FLAG:-false}" = "true" ]; then
        export ENVIRONMENT="production"
    else
        export ENVIRONMENT="development"
        export GCP_PROJECT_ID="${GCP_PROJECT_ID_DEV:-${GCP_PROJECT_ID:-}}"
        echo "[$REPO_NAME] 🟡 BRANCH MODE: using dev project (branch: $CURRENT_BRANCH)"
    fi
fi

# ── EARLY EXIT: identical to main ─────────────────────────────────────────────
git fetch origin main --quiet 2>/dev/null || true
if git rev-parse origin/main &>/dev/null && [ -z "$(git diff origin/main 2>/dev/null)" ]; then
    echo "[$REPO_NAME] No differences from main — nothing to merge"
    exit 0
fi

if [ -z "$(git status --porcelain)" ]; then
    echo "[$REPO_NAME] No changes to commit"
    exit 0
fi

# ============================================================================
# STAGE 3: LOCAL QUALITY GATES (two-phase: auto-fix → verify)
# ============================================================================
echo "=========================================="
echo "STAGE 3: Local Quality Gates"
echo "=========================================="
echo ""

if [ -f "scripts/quality-gates.sh" ]; then
    echo "[$REPO_NAME] Phase 1: auto-fix (ruff format + ruff check --fix)..."
    bash scripts/quality-gates.sh $SKIP_TESTS $SKIP_TYPECHECK

    echo "[$REPO_NAME] Phase 2: verify (--no-fix mode)..."
    if ! bash scripts/quality-gates.sh --no-fix $SKIP_TESTS $SKIP_TYPECHECK; then
        echo "[$REPO_NAME] ❌ Quality gates FAILED — fix remaining issues before merging"
        exit 1
    fi
    echo "[$REPO_NAME] ✅ Quality gates PASSED"
else
    echo "[$REPO_NAME] ⚠️  No quality-gates.sh found (skipping quality gate check)"
fi

echo ""

# ============================================================================
# STAGE 4: ACT SIMULATION (skip with --quick)
# ============================================================================
echo "=========================================="
echo "STAGE 4: Act Simulation"
echo "=========================================="

if [ "$QUICK" = true ]; then
    echo "[$REPO_NAME] --quick: Skipping act simulation"
elif command -v act &>/dev/null; then
    ACT_SECRETS=""
    [ -f ~/.secrets ] && ACT_SECRETS="--secret-file ~/.secrets"

    # Detect container architecture.
    # Apple Silicon (Darwin arm64) needs linux/amd64 emulation for GitHub Actions runners.
    # Linux hosts (including friend devs on x86) use native architecture.
    ACT_ARCH=""
    if [[ "$(uname -s)" == "Darwin" ]] && [[ "$(uname -m)" == "arm64" ]]; then
        ACT_ARCH="--container-architecture linux/amd64"
        echo "[$REPO_NAME] 🍎 Apple Silicon detected — using linux/amd64 emulation"
    elif [[ "$(uname -s)" == "Linux" ]]; then
        echo "[$REPO_NAME] 🐧 Linux detected — using native architecture"
    fi

    # _pull_with_timeout pulls a Docker image in the background, monitoring every 10s.
    # If no progress is detected for STALL_LIMIT seconds, kills the pull and returns 1.
    _pull_with_timeout() {
        local image="$1"
        local stall_limit="${2:-60}"
        local PULL_LOG
        PULL_LOG=$(mktemp)

        echo "[$REPO_NAME] 📦 Pulling $image (stall timeout: ${stall_limit}s)..."
        docker pull "$image" >"$PULL_LOG" 2>&1 &
        local PULL_PID=$!

        local elapsed=0
        local last_size=0
        local stall_secs=0

        while kill -0 "$PULL_PID" 2>/dev/null; do
            sleep 10
            elapsed=$((elapsed + 10))
            local curr_size
            curr_size=$(wc -c <"$PULL_LOG" 2>/dev/null || echo 0)
            if [ "$curr_size" -eq "$last_size" ]; then
                stall_secs=$((stall_secs + 10))
                echo "[$REPO_NAME]   pull: no progress for ${stall_secs}s (${image})"
                if [ "$stall_secs" -ge "$stall_limit" ]; then
                    echo "[$REPO_NAME] ⚠️  Pull stalled ${stall_limit}s — aborting"
                    kill "$PULL_PID" 2>/dev/null
                    rm -f "$PULL_LOG"
                    return 1
                fi
            else
                stall_secs=0
            fi
            last_size=$curr_size
        done

        wait "$PULL_PID"
        local rc=$?
        rm -f "$PULL_LOG"
        return $rc
    }

    # Try images in order: preferred medium runner → micro fallback.
    # Once cached, --pull=false keeps subsequent runs instant.
    ACT_IMAGE_ARGS=""
    PREFERRED_IMAGE="catthehacker/ubuntu:act-22.04"
    MICRO_IMAGE="node:16-buster-slim"

    if docker image inspect "$PREFERRED_IMAGE" &>/dev/null; then
        echo "[$REPO_NAME] ✅ Runner image cached ($PREFERRED_IMAGE) — skipping pull"
    elif _pull_with_timeout "$PREFERRED_IMAGE" 60; then
        echo "[$REPO_NAME] ✅ Pulled $PREFERRED_IMAGE"
    else
        echo "[$REPO_NAME] ⚠️  Preferred image unavailable — trying micro fallback ($MICRO_IMAGE)"
        if docker image inspect "$MICRO_IMAGE" &>/dev/null || _pull_with_timeout "$MICRO_IMAGE" 60; then
            echo "[$REPO_NAME] ✅ Using micro runner: $MICRO_IMAGE"
            ACT_IMAGE_ARGS="-P ubuntu-latest=$MICRO_IMAGE"
        else
            echo "[$REPO_NAME] ⚠️  All image pulls failed — skipping act simulation (CI is authoritative)"
            ACT_IMAGE_ARGS="SKIP"
        fi
    fi

    if [ "$ACT_IMAGE_ARGS" != "SKIP" ]; then
        if act -j quality-gates $ACT_SECRETS $ACT_ARCH --pull=false $ACT_IMAGE_ARGS 2>/dev/null; then
            echo "[$REPO_NAME] ✅ Act simulation PASSED"
        else
            echo "[$REPO_NAME] ⚠️  Act simulation failed (continuing — CI will be the authoritative check)"
        fi
    fi
else
    echo "[$REPO_NAME] ⚠️  act not installed (skipping — install: brew install act)"
fi

echo ""

# ============================================================================
# STAGE 5: CREATE PR
# ============================================================================
echo "=========================================="
echo "STAGE 5: Create PR"
echo "=========================================="
echo ""

# Stash all changes (including untracked) before branch switch
RESTORE_STASH=0
if [ -n "$(git status --porcelain)" ]; then
    echo "[$REPO_NAME] Stashing changes..."
    git stash push -u -m "quickmerge-$$" --quiet
    RESTORE_STASH=1
fi

git fetch origin main --quiet

# Create branch
if [ -n "$DEP_BRANCH" ]; then
    BRANCH="$DEP_BRANCH"
    echo "[$REPO_NAME] Using specified branch: $BRANCH"
else
    BRANCH="auto/$(date +%Y%m%d-%H%M%S)-$$"
    echo "[$REPO_NAME] Creating auto-generated branch: $BRANCH"
fi

git checkout -b "$BRANCH" origin/main --quiet
echo ""

# Restore stash on new branch
if [ "$RESTORE_STASH" = 1 ] && git stash list | grep -q "quickmerge-$$"; then
    git stash pop --quiet
fi

# Stage files: --files for selective add, else add all
sync 2>/dev/null || true
sleep 0.3

if [ -n "$FILES_ARG" ]; then
    ADDED_ANY=0
    for f in $FILES_ARG; do
        if [ -e "$f" ]; then
            git add "$f"
            ADDED_ANY=1
        else
            echo "[$REPO_NAME] ⚠️  Path not found: $f"
        fi
    done
    if [ "$ADDED_ANY" = 0 ]; then
        echo "[$REPO_NAME] ❌ No valid paths from --files. Nothing to commit."
        exit 1
    fi
    if [ -z "$(git diff --cached --name-only)" ]; then
        echo "[$REPO_NAME] ❌ No changes in --files paths. Nothing to commit."
        exit 1
    fi
else
    git add -A
fi

git commit -m "$COMMIT_MSG" --quiet

git push -u origin "$BRANCH" --quiet 2>/dev/null

# Extract issue references from commit message for PR body
ISSUE_REFS=$(echo "$COMMIT_MSG" | grep -oE "(Fixes|Closes|Resolves) [^#]*#[0-9]+" || echo "")
PR_BODY="Automated PR. Will auto-merge once quality gates pass.

${ISSUE_REFS}"

PR_URL=$(gh pr create \
    --title "$COMMIT_MSG" \
    --body "$PR_BODY" \
    --base main \
    --head "$BRANCH" 2>/dev/null)

PR_NUM=$(echo "$PR_URL" | grep -o "[0-9]*$" || echo "")
if [ -n "$PR_NUM" ]; then
    gh pr merge "$PR_NUM" --auto --squash --delete-branch 2>/dev/null || true
fi

echo "[$REPO_NAME] ✅ PR created: $PR_URL (auto-merge enabled)"
echo "[$REPO_NAME] Staying on branch $BRANCH — PR will auto-merge when CI passes"
echo "[$REPO_NAME] To sync with main after merge: git checkout main && git pull"
