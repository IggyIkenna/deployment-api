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
#   1. Dependency validation (workspace-manifest.json)
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
NO_PR=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --files)
      FILES_ARG="$2"
      shift 2
      ;;
    --dep-branch)
      DEP_BRANCH="$2"
      shift 2
      ;;
    --skip-tests)
      SKIP_TESTS="--skip-tests"
      shift
      ;;
    --skip-typecheck)
      SKIP_TYPECHECK="--skip-typecheck"
      shift
      ;;
    --quick)
      QUICK=true
      shift
      ;;
    --no-pr)
      NO_PR=true
      shift
      ;;
    --unit-only)
      QUICK=true
      NO_PR=true
      shift
      ;;
    *)
      COMMIT_MSG="$1"
      shift
      ;;
  esac
done

# NOTE: Cursor rules sync was previously done here as Stage 0 (copy-based).
# Rules are now symlinked (.cursor/rules/ -> unified-trading-pm/cursor-rules/)
# so no sync step is needed — edits go directly to the git-tracked source.

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

# ── EARLY EXIT: nothing to commit (skip when --no-pr) ─────────────────────────────────────────────
git fetch origin main --quiet 2>/dev/null || true
if [ "$NO_PR" != "true" ] && [ -z "$(git status --porcelain)" ] && git diff origin/main --quiet 2>/dev/null; then
  echo "[$REPO_NAME] Nothing to commit — exiting fast"
  exit 0
fi

# ============================================================================
# STAGE 1: DEPENDENCY VALIDATION (workspace-manifest.json SSOT)
# ============================================================================
echo "=========================================="
echo "STAGE 1: Dependency Validation"
echo "=========================================="

MANIFEST_PATH="$WORKSPACE_ROOT/unified-trading-pm/workspace-manifest.json"
if [ -f "$MANIFEST_PATH" ]; then
  DEPS=$(jq -r '.repositories["'"$REPO_NAME"'"].dependencies[]?.name // empty' "$MANIFEST_PATH" 2>/dev/null || echo "")
  if [ -n "$DEPS" ]; then
    echo "Checking dependencies vs origin/main (from workspace-manifest.json)..."
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
      echo "Your local dependency changes are intentional — do NOT discard them."
      echo ""
      echo "Use --dep-branch NAME to create a branch for your dependency changes,"
      echo "then proceed. Quickmerge will cascade changes to the named branch:"
      echo ""
      echo "  bash scripts/quickmerge.sh \"$COMMIT_MSG\" --dep-branch \"my-feature\""
      echo ""
      echo "═══════════════════════════════════════════════════════"
      exit 1
    fi

    if [ -n "$DEP_BRANCH" ]; then
      echo "✅ --dep-branch specified: $DEP_BRANCH (branch isolation mode)"
    fi
  else
    echo "✅ No dependencies for $REPO_NAME (workspace-manifest.json)"
  fi
else
  echo "⚠️  workspace-manifest.json not found at $MANIFEST_PATH (skipping dependency validation)"
fi

echo ""

# ============================================================================
# STAGE 1.5: PM DEPENDENCY ALIGNMENT (unified-trading-pm only)
# ============================================================================
if [ "$REPO_NAME" = "unified-trading-pm" ]; then
  echo "=========================================="
  echo "STAGE 1.5: Dependency Alignment (PM)"
  echo "=========================================="
  ALIGN_SCRIPT="$WORKSPACE_ROOT/unified-trading-pm/scripts/manifest/check-dependency-alignment.py"
  if [ -f "$ALIGN_SCRIPT" ]; then
    cd "$WORKSPACE_ROOT"
    source .venv-workspace/bin/activate 2>/dev/null || true
    python unified-trading-pm/scripts/manifest/generate-derived-manifest.py 2>/dev/null || true
    if python "$ALIGN_SCRIPT" --json 2>/dev/null | grep -q '"aligned": true'; then
      echo "[$REPO_NAME] ✅ Dependency alignment PASSED"
    else
      echo "[$REPO_NAME] ❌ Dependency alignment FAILED"
      echo ""
      echo "Run before pushing PM:"
      echo "  python unified-trading-pm/scripts/manifest/generate-derived-manifest.py"
      echo "  python unified-trading-pm/scripts/manifest/check-dependency-alignment.py --json"
      echo "  python unified-trading-pm/scripts/manifest/fix-internal-dependency-alignment.py --apply  # if internal mismatches"
      echo ""
      echo "See: unified-trading-pm/scripts/manifest/README-DEPENDENCY-ALIGNMENT.md"
      cd "$REPO_DIR"
      exit 1
    fi
    cd "$REPO_DIR"
  fi
  echo ""
fi

# ============================================================================
# STAGE 2: PRE-FLIGHT AUDIT (always runs — never skipped)
# ============================================================================
echo "=========================================="
echo "STAGE 2: Pre-flight Audit"
echo "=========================================="

PREFLIGHT_SCRIPT="$WORKSPACE_ROOT/unified-trading-pm/scripts/validation/pre-flight-audit.sh"
if [ -f "$PREFLIGHT_SCRIPT" ]; then
  if bash "$PREFLIGHT_SCRIPT" "$REPO_NAME"; then
    echo "[$REPO_NAME] ✅ Pre-flight audit PASSED"
  else
    echo "[$REPO_NAME] ❌ Pre-flight audit FAILED"
    exit 1
  fi
else
  echo "[$REPO_NAME] ❌ pre-flight-audit.sh not found at $PREFLIGHT_SCRIPT — required"
  exit 1
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

# ── EARLY EXIT: identical to main (skip when --no-pr) ─────────────────────────────────────────────
git fetch origin main --quiet 2>/dev/null || true
if [ "$NO_PR" != "true" ]; then
  if git rev-parse origin/main &>/dev/null && [ -z "$(git diff origin/main 2>/dev/null)" ]; then
    echo "[$REPO_NAME] No differences from main — nothing to merge"
    exit 0
  fi

  if [ -z "$(git status --porcelain)" ]; then
    echo "[$REPO_NAME] No changes to commit"
    exit 0
  fi
fi

# ============================================================================
# Run setup.sh first to ensure deps (incl. pytest) are installed
if [ -f "scripts/setup.sh" ]; then
  echo "[$REPO_NAME] Ensuring env ready (setup.sh)..."
  bash scripts/setup.sh --check 2>/dev/null || bash scripts/setup.sh
fi

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
  # Strict check: repos that require quality gates must have scripts/quality-gates.sh
  REPO_TYPE=""
  QG_STATUS=""
  if [ -f "${MANIFEST_PATH:-}" ]; then
    REPO_TYPE=$(jq -r '.repositories["'"$REPO_NAME"'"] | .type // empty' "$MANIFEST_PATH" 2>/dev/null)
    QG_STATUS=$(jq -r '.repositories["'"$REPO_NAME"'"] | .quality_gate_status // empty' "$MANIFEST_PATH" 2>/dev/null)
  fi
  QG_REQUIRED_TYPES="library service api-service infrastructure devops test-harness"
  if [ -n "$REPO_TYPE" ] && [ -n "$QG_STATUS" ] && \
     echo "$QG_REQUIRED_TYPES" | grep -qw "$REPO_TYPE" && \
     [ "$QG_STATUS" != "NO_QG" ]; then
    echo "[$REPO_NAME] ❌ quality-gates.sh required: type=$REPO_TYPE, quality_gate_status=$QG_STATUS (add scripts/quality-gates.sh or set quality_gate_status=NO_QG in manifest)" >&2
    exit 1
  fi
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
else
  # Auto-install act if not present (Linux or macOS)
  if ! command -v act &>/dev/null; then
    OS="$(uname -s)"
    echo "[$REPO_NAME] act not found — installing for $OS..."
    if [ "$OS" = "Darwin" ]; then
      if command -v brew &>/dev/null; then
        brew install act
      else
        echo "[$REPO_NAME] ❌ Homebrew not found. Install it first: https://brew.sh" >&2
        exit 1
      fi
    elif [ "$OS" = "Linux" ]; then
      INSTALL_DIR="${HOME}/.local/bin"
      mkdir -p "$INSTALL_DIR"
      curl -fsSL https://raw.githubusercontent.com/nektos/act/master/install.sh | bash -s -- -b "$INSTALL_DIR"
      export PATH="$INSTALL_DIR:$PATH"
    else
      echo "[$REPO_NAME] ❌ Unsupported OS ($OS) — install act manually: https://github.com/nektos/act" >&2
      exit 1
    fi
  fi

  if ! command -v act &>/dev/null; then
    echo "[$REPO_NAME] ❌ act installation failed — cannot run CI simulation" >&2
    exit 1
  fi

  ACT_SECRETS=""
  [ -f ~/.secrets ] && ACT_SECRETS="--secret-file ~/.secrets"
  if act -j quality-gates $ACT_SECRETS 2>/dev/null; then
    echo "[$REPO_NAME] ✅ Act simulation PASSED"
  else
    echo "[$REPO_NAME] ⚠️  Act simulation failed (continuing — CI will be the authoritative check)"
  fi
fi

echo ""

# ============================================================================
# STAGE 5: CREATE PR (skip with --no-pr or --unit-only)
# ============================================================================
if [ "$NO_PR" = true ]; then
  echo "[$REPO_NAME] --no-pr: Skipping PR creation"
else
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
fi
