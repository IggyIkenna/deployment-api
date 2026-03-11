#!/bin/bash
# CANONICAL QUICKMERGE — unified-trading-system
#
# Single source of truth for all repos. Copy to scripts/quickmerge.sh.
#
# Three-tier branch model:
#   feat/*    → QG only, no PR (feature iteration, auto-detected)
#   staging   → convergence zone for breaking changes; PR targets main after SIT
#   main      → always stable; NEVER a direct target for breaking changes
#
# Usage:
#   ./scripts/quickmerge.sh "commit message"
#   ./scripts/quickmerge.sh "commit message" --files "path1 path2 path3"
#   ./scripts/quickmerge.sh "commit message" --dep-branch "my-feature"  # human-only; overrides manifest branch
#   ./scripts/quickmerge.sh "commit message" --to-staging
#
# Branch resolution (no --dep-branch):
#   1. Read active_feature_branch from unified-trading-pm/workspace-manifest.json
#   2. Fallback to auto/YYYYMMDD-HHMMSS-$$ if manifest has no entry
#   Currently: active_feature_branch = live-defi-rollout (set in manifest)
#   ./scripts/quickmerge.sh "commit message" --quick
#   ./scripts/quickmerge.sh "commit message" --skip-tests
#   ./scripts/quickmerge.sh "commit message" --skip-typecheck
#
# Flags:
#   --files "p1 p2"    Stage only these paths (multi-agent: avoid committing other agents' work)
#   --dep-branch NAME  HUMAN-ONLY. Branch isolation when deps have uncommitted changes that aren't yet
#                      on main. Agents MUST NOT use this — they use active_feature_branch from manifest.
#   --to-staging       Breaking change path: PR targets staging instead of main; checks staging lock.
#                      dep-branch auto-derived from current git branch. Mutually exclusive with --dep-branch.
#   --quick            Skip only act simulation (Stage 4); all other checks run
#   --skip-tests       Pass --skip-tests to quality-gates.sh (lint+type+codex only)
#   --skip-typecheck   Pass --skip-typecheck to quality-gates.sh (skips basedpyright only)
#   --skip-codex       Skip codex compliance check (Stage 3 §5). Human-only escape hatch; never use with --agent.
#   --skip-preflight   Skip pre-flight audit (Stage 2). Human-only escape hatch; never use with --agent.
#   --user-approved    Deprecated — Stage 0.3 is advisory-only; no gate to bypass.
#                      Version bumps are GHA-only (semver-agent.yml). Kept for backwards compat.
#
# When to use --to-staging:
#   feat!: / BREAKING CHANGE: commits that break downstream API contracts.
#   All other commits (fix:, feat:, chore:) go directly to main (no --to-staging needed).
#   See: unified-trading-pm/docs/repo-management/version-cascade-flow.md
#
# Pipeline:
#   1. Dependency validation (workspace-manifest.json)
#   1.5. PM: dependency alignment check; ALL: staging lock check (if --to-staging)
#   2. Pre-flight audit (skippable with --skip-preflight for multi-agent use)
#   3. Local quality gates (two-phase: auto-fix → verify)
#   4. Act simulation (default; skip with --quick)
#   5. Create PR + enable auto-merge (base: staging if --to-staging, else main)
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

# Workspace root: parent of repo containing this script. Check both levels so it works when run from
# repo root (bash scripts/quickmerge.sh) or workspace root (bash unified-trading-pm/scripts/quickmerge.sh).
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
WORKSPACE_ROOT="$(cd "$REPO_ROOT/.." && pwd)"
# Fallback: .act-secrets at repo root (e.g. single-repo dev)
[ ! -f "${WORKSPACE_ROOT}/.act-secrets" ] && [ -f "${REPO_ROOT}/.act-secrets" ] && WORKSPACE_ROOT="$REPO_ROOT"

# Act secrets: prefer UNIFIED_TRADING_WORKSPACE_ROOT when set (portable across team); else use computed WORKSPACE_ROOT
ACT_SECRETS_ROOT="${UNIFIED_TRADING_WORKSPACE_ROOT:-$WORKSPACE_ROOT}"
[ -f "${ACT_SECRETS_ROOT}/.act-secrets" ] && export ACT_SECRETS_FILE="${ACT_SECRETS_ROOT}/.act-secrets"

# ── PARSE ARGUMENTS ───────────────────────────────────────────────────────────
COMMIT_MSG="chore: automated update"
FILES_ARG=""
DEP_BRANCH=""
TO_STAGING=false
SKIP_TESTS=""
SKIP_TYPECHECK=""
QUICK=false
NO_PR=false
SKIP_CODEX=""
SKIP_PREFLIGHT=false
USER_APPROVED=false
AGENT_MODE=false

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
    --to-staging)
      TO_STAGING=true
      shift
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
    --skip-codex)
      SKIP_CODEX="--skip-codex"
      shift
      ;;
    --skip-preflight)
      SKIP_PREFLIGHT=true
      shift
      ;;
    --user-approved)
      USER_APPROVED=true
      shift
      ;;
    --agent)
      AGENT_MODE=true
      shift
      ;;
    *)
      COMMIT_MSG="$1"
      shift
      ;;
  esac
done

# --agent implicitly skips tests (lightweight: lint+format+typecheck+codex only)
[ "$AGENT_MODE" = true ] && SKIP_TESTS="--skip-tests"

# ── FLAG VALIDATION ────────────────────────────────────────────────────────────
# --dep-branch is HUMAN-ONLY. Agents must never pass it — they rely on active_feature_branch
# from workspace-manifest.json. Fail loud so agents don't silently use the wrong branch.
if [ "$AGENT_MODE" = true ] && [ -n "$DEP_BRANCH" ]; then
  echo "❌ --dep-branch is not allowed in --agent mode."
  echo "   Agents use active_feature_branch from workspace-manifest.json automatically."
  echo "   Remove --dep-branch from your quickmerge call."
  exit 1
fi

if [ "$TO_STAGING" = true ] && [ -n "$DEP_BRANCH" ]; then
  echo "❌ --to-staging and --dep-branch are mutually exclusive."
  echo "   --to-staging auto-derives the dep-branch from your current git branch."
  echo "   Remove --dep-branch and re-run."
  exit 1
fi

# Auto-derive dep-branch from current git branch when --to-staging
CURRENT_BRANCH_PRE=$(git branch --show-current 2>/dev/null || echo "")
if [ "$TO_STAGING" = true ] && [ -n "$CURRENT_BRANCH_PRE" ] && [ "$CURRENT_BRANCH_PRE" != "main" ] && [ "$CURRENT_BRANCH_PRE" != "staging" ]; then
  DEP_BRANCH="$CURRENT_BRANCH_PRE"
  echo "[$REPO_NAME] --to-staging: auto-derived dep-branch from current branch: $DEP_BRANCH"
fi

# Breaking change warning: if no --to-staging but commit looks like breaking change
if [ "$TO_STAGING" = false ] && [ "$NO_PR" = false ]; then
  FIRST_LINE=$(echo "$COMMIT_MSG" | head -n1)
  if echo "$COMMIT_MSG" | grep -q "BREAKING CHANGE:" || echo "$FIRST_LINE" | grep -qE "^[a-z]+!\("; then
    echo ""
    echo "⚠️  WARNING: This commit appears to be a breaking change (feat!: or BREAKING CHANGE:)."
    echo "   Breaking changes should target staging via --to-staging so downstream repos"
    echo "   can run quality gates before the change reaches main."
    echo "   To use the breaking change path: bash scripts/quickmerge.sh \"$COMMIT_MSG\" --to-staging"
    echo "   Continuing with direct-to-main path (your choice)."
    echo ""
  fi
fi

# NOTE: Cursor rules sync was previously done here as Stage 0 (copy-based).
# Rules are now symlinked (.cursor/rules/ -> unified-trading-pm/.cursor/rules/)
# so no sync step is needed — edits go directly to the git-tracked source.

REPO_DIR="$(git rev-parse --show-toplevel 2>/dev/null || echo "$REPO_ROOT")"
REPO_DIR="${REPO_DIR:-$REPO_ROOT}"
REPO_NAME=$(basename "$REPO_DIR")
cd "$REPO_DIR"

# ── CASCADE DEP BRANCH ────────────────────────────────────────────────────────
# When --dep-branch is set, walk the full transitive ancestor chain (DAG upward
# from REPO_NAME) and switch each ancestor repo to the named branch before Stage 1.
# This ensures Stage 1 dependency validation passes when ancestor repos have local
# changes that haven't been pushed to main yet.
#
# Rules:
#   - Only ancestors of the changed repo are touched (not siblings or unrelated repos)
#   - If an ancestor has local changes, they are stashed, branch is created/switched,
#     and the stash is re-applied on the new branch
#   - If an ancestor doesn't exist locally, it is skipped (non-fatal)
#   - No version bumping is done (version bumping is only on main via semver-agent)
cascade_dep_branch() {
  local branch_name="$1"
  local manifest_path="$WORKSPACE_ROOT/unified-trading-pm/workspace-manifest.json"

  [ -f "$manifest_path" ] || { echo "[cascade] ⚠️  Manifest not found at $manifest_path — skipping cascade"; return 0; }

  echo "=========================================="
  echo "STAGE 0: Cascade dep-branch '$branch_name' to transitive ancestors"
  echo "=========================================="

  # Walk the DAG upward from REPO_NAME to collect all transitive ancestors.
  # Output: one repo name per line, deepest deps first (reverse BFS order).
  local ancestors
  ancestors=$(python3.13 - "$manifest_path" "$REPO_NAME" 2>/dev/null <<'PYEOF'
import json, sys
from collections import deque

manifest_path, repo_name = sys.argv[1], sys.argv[2]
with open(manifest_path) as f:
    manifest = json.load(f)

repos = manifest.get("repositories", {})

def get_internal_deps(name):
    repo = repos.get(name, {})
    # Support both "internal_dependencies" and "dependencies" key names
    raw_deps = repo.get("internal_dependencies") or repo.get("dependencies") or []
    result = []
    for d in raw_deps:
        dep_name = d.get("name", "") if isinstance(d, dict) else str(d)
        if dep_name and dep_name in repos:
            result.append(dep_name)
    return result

# BFS to collect all transitive ancestors
visited, queue, order = set(), deque(get_internal_deps(repo_name)), []
while queue:
    name = queue.popleft()
    if name not in visited:
        visited.add(name)
        order.append(name)
        queue.extend(get_internal_deps(name))

# Reverse: deepest deps first (so T0 libs are processed before T1, etc.)
for name in reversed(order):
    print(name)
PYEOF
)

  if [ -z "$ancestors" ]; then
    echo "[cascade] No transitive ancestors found for $REPO_NAME — nothing to cascade"
    echo ""
    return 0
  fi

  echo "[cascade] Ancestors of $REPO_NAME (deepest first):"
  while IFS= read -r a; do [ -n "$a" ] && echo "  - $a"; done <<< "$ancestors"
  echo ""

  local cascaded=0

  while IFS= read -r ancestor; do
    [ -z "$ancestor" ] && continue
    local ancestor_path="$WORKSPACE_ROOT/$ancestor"

    if [ ! -d "$ancestor_path" ]; then
      echo "[cascade] ⏭️  $ancestor: not found locally — skipping"
      continue
    fi

    echo "[cascade] 🔀 $ancestor → branch '$branch_name'..."
    (
      cd "$ancestor_path" || exit 1
      git fetch origin main --quiet 2>/dev/null || true

      # Stash local changes if any
      local_changes=$(git status --porcelain 2>/dev/null | wc -l | tr -d ' ')
      if [ "$local_changes" -gt 0 ]; then
        git stash push -u -m "cascade-$$-$branch_name" --quiet 2>/dev/null || true
        stashed=1
      else
        stashed=0
      fi

      # Switch to (or create) the branch
      if git show-ref --verify --quiet "refs/remotes/origin/$branch_name" 2>/dev/null; then
        git checkout -B "$branch_name" "origin/$branch_name" --quiet 2>/dev/null || \
          git checkout "$branch_name" --quiet 2>/dev/null || \
          git checkout -b "$branch_name" origin/main --quiet
      elif git show-ref --verify --quiet "refs/heads/$branch_name" 2>/dev/null; then
        git checkout "$branch_name" --quiet
      else
        git checkout -b "$branch_name" origin/main --quiet
      fi

      # Restore stash on the new branch
      if [ "$stashed" = 1 ] && git stash list 2>/dev/null | grep -q "cascade-$$-$branch_name"; then
        git stash pop --quiet 2>/dev/null || \
          echo "[cascade] ⚠️  $ancestor: stash pop had conflicts — resolve manually before committing"
      fi
    )
    echo "[cascade] ✅ $ancestor on branch '$branch_name'"
    cascaded=$((cascaded + 1))
    echo ""
  done <<< "$ancestors"

  echo "[cascade] ✅ Cascaded $cascaded ancestor(s) to branch '$branch_name'"
  echo ""
}

# Cascade dep-branch before any validation stages
[ -n "$DEP_BRANCH" ] && cascade_dep_branch "$DEP_BRANCH"

# ── ACTIVATE VENV ─────────────────────────────────────────────────────────────
# USE_WORKSPACE_VENV=1: prefer .venv-workspace over repo .venv (workspace-venv-fallback.mdc)
VENV_ACTIVATED=0
if [ "${USE_WORKSPACE_VENV:-0}" = "1" ] && [ -f "${WORKSPACE_ROOT}/.venv-workspace/bin/activate" ]; then
  source "${WORKSPACE_ROOT}/.venv-workspace/bin/activate"
  VENV_ACTIVATED=1
  echo "[$REPO_NAME] Using .venv-workspace (Python $(python --version 2>&1))"
elif [ -f ".venv/bin/activate" ]; then
  source .venv/bin/activate
  VENV_ACTIVATED=1
  echo "[$REPO_NAME] Using .venv (Python $(python --version 2>&1))"
elif [ -f ".venv/Scripts/activate" ]; then
  source .venv/Scripts/activate
  VENV_ACTIVATED=1
  echo "[$REPO_NAME] Using .venv (Python $(python --version 2>&1))"
elif [ -f "${WORKSPACE_ROOT}/.venv-workspace/bin/activate" ]; then
  source "${WORKSPACE_ROOT}/.venv-workspace/bin/activate"
  VENV_ACTIVATED=1
  echo "[$REPO_NAME] Using .venv-workspace (Python $(python --version 2>&1))"
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
# --- Stage 0.3: Major bump advisory (informational only — no blocking) ---
# Version bumps are GHA-only. semver-agent.yml is the sole authority for
# classifying PATCH/MINOR/MAJOR after QG passes on staging.
# quickmerge.sh NEVER bumps versions and NEVER blocks on version concerns.
# This stage only prints an advisory so the author is aware that semver-agent
# will classify this commit as a potential MAJOR bump and may open an approval
# issue if the repo is post-1.0.0.
# ============================================================================
echo "=========================================="
echo "STAGE 0.3: Semver Advisory (informational — no blocking)"
echo "=========================================="

_FIRST_LINE_MSG=$(printf '%s' "$COMMIT_MSG" | head -n1)
_IS_FEAT_BREAKING=false
if printf '%s' "$_FIRST_LINE_MSG" | grep -qiE "^feat!(\(.*\))?:"; then
  _IS_FEAT_BREAKING=true
fi

if [ "$_IS_FEAT_BREAKING" = "true" ]; then
  # Read current version for informational display only
  _CURRENT_VERSION=""
  _REPO_LABEL="$REPO_NAME"
  if [ -f "pyproject.toml" ]; then
    _CURRENT_VERSION=$(grep -E '^version = ' pyproject.toml | head -1 | sed 's/version = "//;s/"//' 2>/dev/null || echo "")
    _REPO_LABEL=$(grep -E '^name = ' pyproject.toml | head -1 | sed 's/name = "//;s/"//' 2>/dev/null || echo "$REPO_NAME")
  elif [ -f "package.json" ] && command -v node &>/dev/null; then
    _CURRENT_VERSION=$(node -e "console.log(require('./package.json').version)" 2>/dev/null || echo "")
    _REPO_LABEL=$(node -e "console.log(require('./package.json').name)" 2>/dev/null || echo "$REPO_NAME")
  fi

  if [ -n "$_CURRENT_VERSION" ]; then
    _MAJOR_COMPONENT=$(printf '%s' "$_CURRENT_VERSION" | cut -d. -f1)
    if [ -n "$_MAJOR_COMPONENT" ] && [ "$_MAJOR_COMPONENT" -ge 1 ] 2>/dev/null; then
      echo "[$REPO_NAME] ℹ️  Stage 0.3: feat!: on post-1.0.0 repo detected (current: $_CURRENT_VERSION)"
      echo "[$REPO_NAME]    NOTE: Version bumps are handled by semver-agent.yml after QG passes on staging."
      echo "[$REPO_NAME]    semver-agent will open a major-bump-approval issue for human sign-off."
      echo "[$REPO_NAME]    No manual version changes needed — do NOT edit pyproject.toml version manually."
    else
      echo "[$REPO_NAME] ℹ️  Stage 0.3: feat!: on pre-1.0.0 repo ($_CURRENT_VERSION) — semver-agent will bump MINOR"
    fi
  else
    echo "[$REPO_NAME] ℹ️  Stage 0.3: feat!: commit — semver-agent will classify version bump after QG on staging"
  fi
else
  echo "[$REPO_NAME] ✅ Stage 0.3: not a feat!: commit — semver-agent will classify as MINOR or PATCH"
fi

echo ""

# ============================================================================
# STAGE 0.5: PM MANIFEST STALENESS CHECK
# Fetches origin/main of unified-trading-pm; warns if local PM is behind remote.
# In CI auto-pulls (ff-only); interactive mode warns and continues.
# Prevents stale-manifest quickmerges where the local manifest is behind the
# remote, causing constraint mismatches in downstream repos after merge.
# ============================================================================
echo "=========================================="
echo "STAGE 0.5: PM Manifest Staleness Check"
echo "=========================================="
PM_CHECK_PATH="$WORKSPACE_ROOT/unified-trading-pm"
if [ -d "$PM_CHECK_PATH" ] && [ "$REPO_NAME" != "unified-trading-pm" ]; then
  cd "$PM_CHECK_PATH"
  git fetch origin main --quiet 2>/dev/null || true
  LOCAL_PM_HASH=$(git rev-parse HEAD 2>/dev/null || echo "")
  REMOTE_PM_HASH=$(git rev-parse origin/main 2>/dev/null || echo "")
  if [ -n "$LOCAL_PM_HASH" ] && [ -n "$REMOTE_PM_HASH" ] && [ "$LOCAL_PM_HASH" != "$REMOTE_PM_HASH" ]; then
    COMMITS_BEHIND=$(git rev-list HEAD..origin/main --count 2>/dev/null || echo "?")
    echo "⚠️  unified-trading-pm is $COMMITS_BEHIND commit(s) behind origin/main"
    echo "   Local:  $LOCAL_PM_HASH"
    echo "   Remote: $REMOTE_PM_HASH"
    if [ "${CI:-}" = "true" ] || [ "${GITHUB_ACTIONS:-}" = "true" ]; then
      echo "   [CI] Auto-pulling PM to latest..."
      git pull --ff-only origin main --quiet 2>/dev/null && \
        echo "   [$REPO_NAME] ✅ PM pulled to latest" || \
        echo "   [$REPO_NAME] ⚠️  PM pull failed — continuing with stale manifest"
    else
      echo "   To sync: cd unified-trading-pm && git pull origin main"
      echo "   Continuing with local manifest (possible constraint mismatches downstream)."
    fi
  else
    echo "[$REPO_NAME] ✅ unified-trading-pm is current"
  fi
  cd "$REPO_DIR"
elif [ "$REPO_NAME" = "unified-trading-pm" ]; then
  echo "[$REPO_NAME] ✅ Running from PM itself — skipping self-check"
fi
echo ""

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
      echo "── If you are a HUMAN developer ──────────────────────────────────────"
      echo "Use --dep-branch NAME (human-only flag) to create a feature branch for"
      echo "your dependency changes. Quickmerge will cascade to that named branch:"
      echo ""
      echo "  bash scripts/quickmerge.sh \"$COMMIT_MSG\" --dep-branch \"my-feature\""
      echo ""
      echo "── If you are an AGENT ────────────────────────────────────────────────"
      echo "Do NOT use --dep-branch. Commit the dependency changes first (in the"
      echo "dep repo) using the active_feature_branch from workspace-manifest.json,"
      echo "then re-run quickmerge in this repo."
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
# STAGE 1.5: STAGING LOCK CHECK (--to-staging only) + PM DEPENDENCY ALIGNMENT
# ============================================================================
if [ "$TO_STAGING" = true ] && [ -f "$MANIFEST_PATH" ]; then
  echo "=========================================="
  echo "STAGE 1.5: Staging Lock Check"
  echo "=========================================="
  STAGING_LOCKED=$(python3.13 -c "
import json, sys
try:
    with open('${MANIFEST_PATH}') as f:
        m = json.load(f)
    ss = m.get('staging_status', {})
    locked = ss.get('locked', False)
    reason = ss.get('locked_reason') or ''
    since = ss.get('locked_since') or ''
    print(f'locked={str(locked).lower()}')
    print(f'reason={reason}')
    print(f'since={since}')
except Exception as e:
    print(f'locked=false')
" 2>/dev/null)

  IS_LOCKED=$(echo "$STAGING_LOCKED" | grep 'locked=' | cut -d= -f2)
  LOCK_REASON=$(echo "$STAGING_LOCKED" | grep 'reason=' | cut -d= -f2-)
  LOCK_SINCE=$(echo "$STAGING_LOCKED" | grep 'since=' | cut -d= -f2-)

  if [ "$IS_LOCKED" = "true" ]; then
    echo ""
    echo "═══════════════════════════════════════════════════════"
    echo "⏳ STAGING LOCKED — Cannot proceed with --to-staging"
    echo "═══════════════════════════════════════════════════════"
    echo ""
    echo "Reason: $LOCK_REASON"
    echo "Since:  $LOCK_SINCE"
    echo ""
    echo "Staging is locked while a breaking change cascade is converging."
    echo "Wait for the SIT to validate staging and unlock it, then re-run."
    echo ""
    echo "To check staging status:"
    echo "  jq '.staging_status' unified-trading-pm/workspace-manifest.json"
    echo ""
    echo "Your changes are safe — nothing was committed. Re-run when staging clears."
    echo "═══════════════════════════════════════════════════════"
    exit 1
  fi
  echo "[$REPO_NAME] ✅ Staging is unlocked — proceeding"
  echo ""
fi

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
      echo "  python unified-trading-pm/scripts/manifest/fix_external_dependency_alignment.py --apply  # if external mismatches (updates repos to match canonical)"
      echo ""
      echo "See: unified-trading-pm/scripts/manifest/README-DEPENDENCY-ALIGNMENT.md"
      cd "$REPO_DIR"
      exit 1
    fi
    cd "$REPO_DIR"
  fi

  # Regenerate SVG when manifest has been updated (so diagram always reflects current state)
  SVG_SCRIPT="$WORKSPACE_ROOT/unified-trading-pm/scripts/manifest/generate_workspace_dag.py"
  if [ -f "$SVG_SCRIPT" ]; then
    cd "$WORKSPACE_ROOT"
    python "$SVG_SCRIPT" 2>/dev/null && echo "[$REPO_NAME] ✅ Workspace DAG SVG regenerated" || echo "[$REPO_NAME] ⚠️  SVG generation failed (non-blocking)"
    cd "$REPO_DIR"
  fi

  echo ""
fi

# ============================================================================
# STAGE 2: PRE-FLIGHT AUDIT (skippable with --skip-preflight for multi-agent use)
# ============================================================================
echo "=========================================="
echo "STAGE 2: Pre-flight Audit"
echo "=========================================="

if [ "$SKIP_PREFLIGHT" = "true" ]; then
  echo "[$REPO_NAME] ⚠️  Pre-flight audit SKIPPED (--skip-preflight)"
else
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

# Ensure scripts are executable before quality gates (so executable checks pass)
# and stage them so Git records the mode for everyone on commit
for s in scripts/quickmerge.sh scripts/quality-gates.sh; do
  [ -f "$s" ] && chmod +x "$s" && git add "$s" 2>/dev/null || true
done

# STAGE 3: LOCAL QUALITY GATES (two-phase: auto-fix → verify)
# ============================================================================
echo "=========================================="
echo "STAGE 3: Local Quality Gates"
echo "=========================================="
echo ""

if [ -f "scripts/quality-gates.sh" ]; then
  echo "[$REPO_NAME] Phase 1: auto-fix (ruff format + ruff check --fix)..."
  bash scripts/quality-gates.sh $SKIP_TESTS $SKIP_TYPECHECK $SKIP_CODEX

  echo "[$REPO_NAME] Phase 2: verify (--no-fix mode)..."
  if ! bash scripts/quality-gates.sh --no-fix $SKIP_TESTS $SKIP_TYPECHECK $SKIP_CODEX; then
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

if [ "$QUICK" = true ] || [ "$AGENT_MODE" = true ]; then
  echo "[$REPO_NAME] $([ "$AGENT_MODE" = true ] && echo '--agent' || echo '--quick'): Skipping act simulation"
elif [ -f "scripts/quality-gates.sh" ]; then
  if bash scripts/quality-gates.sh --act; then
    echo "[$REPO_NAME] ✅ Act simulation PASSED"
  else
    echo "[$REPO_NAME] ❌ Act simulation FAILED" >&2
    exit 1
  fi
else
  echo "[$REPO_NAME] ⚠️  No scripts/quality-gates.sh — skipping act simulation"
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

# Clean ephemeral cursor files before PR (avoid committing them)
if [ -f "${WORKSPACE_ROOT}/.cleanup-cursor-rules.sh" ]; then
  bash "${WORKSPACE_ROOT}/.cleanup-cursor-rules.sh" 2>/dev/null || true
fi

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
  if git show-ref --verify --quiet "refs/heads/$BRANCH" 2>/dev/null; then
    git checkout "$BRANCH" --quiet
  elif git show-ref --verify --quiet "refs/remotes/origin/$BRANCH" 2>/dev/null; then
    git checkout -B "$BRANCH" "origin/$BRANCH" --quiet
  else
    git checkout -b "$BRANCH" origin/main --quiet 2>/dev/null || git checkout "$BRANCH" --quiet
  fi
else
  # Read active_feature_branch from PM manifest (SSOT for feature branch name)
  MANIFEST_PATH="$WORKSPACE_ROOT/unified-trading-pm/workspace-manifest.json"
  MANIFEST_BRANCH=""
  if [ -f "$MANIFEST_PATH" ]; then
    MANIFEST_BRANCH=$(python3 -c "import json; m=json.load(open('$MANIFEST_PATH')); print(m.get('active_feature_branch',''))" 2>/dev/null || echo "")
  fi
  if [ -n "$MANIFEST_BRANCH" ]; then
    BRANCH="$MANIFEST_BRANCH"
    echo "[$REPO_NAME] Using active_feature_branch from manifest: $BRANCH"
  else
    BRANCH="auto/$(TZ=UTC date +%Y%m%d-%H%M%S)-$$"
    echo "[$REPO_NAME] Creating auto-generated branch: $BRANCH"
  fi
  if git show-ref --verify --quiet "refs/heads/$BRANCH" 2>/dev/null; then
    git checkout "$BRANCH" --quiet
  elif git show-ref --verify --quiet "refs/remotes/origin/$BRANCH" 2>/dev/null; then
    git checkout -B "$BRANCH" "origin/$BRANCH" --quiet
  else
    git checkout -b "$BRANCH" origin/main --quiet 2>/dev/null || git checkout "$BRANCH" --quiet
  fi
fi
echo ""

# Restore stash on new branch
if [ "$RESTORE_STASH" = 1 ] && git stash list | grep -q "quickmerge-$$"; then
  git stash pop --quiet
fi

# Auto-format with Prettier BEFORE staging (so pre-commit validation passes)
# Run twice to handle idempotency
if [ -f ".pre-commit-config.yaml" ] && grep -q "mirrors-prettier" .pre-commit-config.yaml 2>/dev/null; then
  if command -v pre-commit &>/dev/null; then
    pre-commit run prettier --all-files 2>/dev/null || true
    pre-commit run prettier --all-files 2>/dev/null || true
  else
    npx --yes prettier@3.6.2 --write "**/*.{ts,tsx,js,jsx,json,md,yaml,yml,css}" --ignore-unknown 2>/dev/null || true
    npx --yes prettier@3.6.2 --write "**/*.{ts,tsx,js,jsx,json,md,yaml,yml,css}" --ignore-unknown 2>/dev/null || true
  fi
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

if ! git commit -m "$COMMIT_MSG" --quiet; then
  # Pre-commit may have modified files (e.g. Prettier). Stage and retry once.
  git add -A
  if ! git commit -m "$COMMIT_MSG" --quiet; then
    echo "[$REPO_NAME] ❌ Commit failed (pre-commit may have failed). Run: pre-commit run --all-files; git add -A; git commit -m \"...\"" >&2
    exit 1
  fi
  echo "[$REPO_NAME] Pre-commit modified files; staged and committed on retry" >&2
fi

git push -u origin "$BRANCH" --quiet 2>/dev/null

# Extract issue references from commit message for PR body
ISSUE_REFS=$(echo "$COMMIT_MSG" | grep -oE "(Fixes|Closes|Resolves) [^#]*#[0-9]+" || echo "")
PR_BODY="Automated PR. Will auto-merge once quality gates pass.

${ISSUE_REFS}"

# Check staging lock status and inform user (do not abort — GitHub auto-merge queue will hold the PR)
if [ "$TO_STAGING" = true ]; then
  MANIFEST_PATH="${WORKSPACE_ROOT}/unified-trading-pm/workspace-manifest.json"
  if [ -f "$MANIFEST_PATH" ]; then
    LOCKED=$(python3 -c "import json; m=json.load(open('$MANIFEST_PATH')); print(str(m.get('staging_status', {}).get('locked', False)).lower())" 2>/dev/null || echo "false")
    if [ "$LOCKED" = "true" ]; then
      LOCK_REASON=$(python3 -c "import json; m=json.load(open('$MANIFEST_PATH')); print(m.get('staging_status', {}).get('locked_reason', 'unknown'))" 2>/dev/null || echo "unknown")
      LOCK_SINCE=$(python3 -c "import json; m=json.load(open('$MANIFEST_PATH')); print(m.get('staging_status', {}).get('locked_since', 'unknown'))" 2>/dev/null || echo "unknown")
      echo "⚠️  [$REPO_NAME] Staging is locked: \"${LOCK_REASON}\" (since ${LOCK_SINCE})."
      echo "⚠️  [$REPO_NAME] Your --to-staging PR will queue automatically via GitHub's staging-gate check."
      echo "⚠️  [$REPO_NAME] PR creation will proceed — GitHub will hold it until SIT completes."
    fi
  fi
fi

# Determine PR base branch
if [ "$TO_STAGING" = true ]; then
  PR_BASE="staging"
  echo "[$REPO_NAME] --to-staging: PR targets staging (breaking change path)"
else
  PR_BASE="main"
fi

PR_URL=$(gh pr create \
  --title "$COMMIT_MSG" \
  --body "$PR_BODY" \
  --base "$PR_BASE" \
  --head "$BRANCH" 2>/dev/null)

PR_NUM=$(echo "$PR_URL" | grep -o "[0-9]*$" || echo "")
if [ -n "$PR_NUM" ]; then
  if [ "$TO_STAGING" = true ]; then
    # Breaking change path: auto-merge to staging; SIT will validate before promoting to main
    gh pr merge "$PR_NUM" --auto --squash --delete-branch 2>/dev/null || true
    echo "[$REPO_NAME] ✅ PR created targeting staging: $PR_URL (auto-merge to staging enabled)"
    echo "[$REPO_NAME] After staging merge: version-bump.yml will dispatch to PM → cascade to dependents"
    echo "[$REPO_NAME] SIT will validate staging → staging-to-main.yml will promote to main when ready"
  else
    gh pr merge "$PR_NUM" --auto --squash --delete-branch 2>/dev/null || true
    echo "[$REPO_NAME] ✅ PR created: $PR_URL (auto-merge enabled)"
    echo "[$REPO_NAME] Staying on branch $BRANCH — PR will auto-merge when CI passes"
    echo "[$REPO_NAME] To sync with main after merge: git checkout main && git pull"
  fi
fi
fi
