# Dockerfile for deployment-api (FastAPI)
# Production image. Test-in-image uses api-dev stage for quality gates.
#
# UI bundling (2026-04-29): Stage 0 builds the deployment-ui SPA into a
# static dist/ which the api stage copies into ./ui/dist/. This consolidates
# the previously-split unified-trading-deployment-v2 layout (which had its
# own forked api/ + ui/) into the workspace canonicals. main.py's existing
# SPA fallback (StaticFiles mount + catch-all) serves the bundle alongside
# the API on port 8080.
#
# Cloud Build must populate the ./ui/ directory before docker build — clone
# IggyIkenna/deployment-ui at the same SHA / branch as deployment-api, then
# `gcloud builds submit . --substitutions=...`. Local builds: symlink
# ../deployment-ui as ./ui (the COPY at the bottom resolves the symlink).

ARG PROJECT_ID

# Digest-pinned UTL base image (QG STEP 5.79). Declared GLOBAL — before the first FROM — so the
# default digest resolves for the `base` FROM below even on AWS CodeBuild, which (unlike GCP
# cloudbuild) does NOT pass --build-arg BASE_IMAGE_DIGEST. An ARG declared after a FROM is
# stage-scoped and invisible to a later FROM → empty digest → "invalid reference format".
# Refreshed by update-dependency-version.yml on base-image republish.
ARG BASE_IMAGE_DIGEST=sha256:866db4e85d8145a7d2c3e79d7e1d925fbfc6ceaa4e1fee262fb379059ed63ef3
# ── Stage 0: build deployment-ui static bundle ─────────────────────────
FROM public.ecr.aws/docker/library/node:20-slim@sha256:3d0f05455dea2c82e2f76e7e2543964c30f6b7d673fc1a83286736d44fe4c41c AS ui-builder
WORKDIR /app/ui

# Build context expects ./ui/ to be the deployment-ui repo root (populated
# by cloudbuild git-clone or local symlink). deployment-ui migrated npm→pnpm
# 2026-07-29 (deployment-ui@de5b7af2bd, "fleet-standard package manager") —
# package-lock.json no longer exists upstream, pnpm-lock.yaml is now the
# lockfile of record. `npm ci` here breaks with "can only install with an
# existing package-lock.json"; mirror deployment-ui's own Dockerfile (pnpm@10
# via corepack-free `npm install -g pnpm`). package.json + lockfiles first
# for cache.
RUN npm install -g pnpm@10
COPY ui/package.json ui/pnpm-lock.yaml ui/pnpm-workspace.yaml ./
RUN pnpm install --frozen-lockfile

COPY ui/ ./
# Tier-3 shared deploy: bake auth-skip into the SPA so the new Cloud Run
# origin doesn't need to be whitelisted in the workspace's Google OAuth
# client (would otherwise error 401: invalid_client). Vite envs are
# build-time only — must be set BEFORE ``pnpm run build``.
ENV VITE_SKIP_AUTH=true \
    VITE_MOCK_API=false
RUN pnpm run build

# ── Stage 1: Python API base ────────────────────────────────────────────
# BASE_IMAGE_DIGEST is declared GLOBALLY at the top (see the note there — it must precede the
# first FROM to be visible here). cloudbuild may override: --build-arg BASE_IMAGE_DIGEST=sha256:...
FROM --platform=linux/amd64 asia-northeast1-docker.pkg.dev/${PROJECT_ID}/unified-trading-library/unified-trading-library@${BASE_IMAGE_DIGEST} AS base

FROM base AS api
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends ripgrep tini \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock ./
# UTL base image already has unified-trading-library + unified-api-contracts +
# unified-cloud-interface preinstalled. deployment-api just adds its web /
# auth / cache extras. We install them explicitly rather than via
# ``uv pip install .`` because pyproject.toml's ``[tool.uv.sources]`` block
# points at sibling repos (``../unified-trading-library`` etc.) that don't
# exist in the Cloud Build context. The previous ``uv sync --frozen --no-dev
# --system`` variant has been broken since 2026-04-29 (``--system`` is not a
# valid ``uv sync`` flag — it belongs to ``uv pip install`` / ``uv pip sync``)
# and is what's been keeping the auto-CI red.
RUN uv pip install --system --no-cache-dir \
      'fastapi<1.0.0,>=0.115.0' \
      'uvicorn[standard]>=0.27.0,<1.0.0' \
      'gunicorn>=22.0.0,<23.0.0' \
      'pydantic>=2.12.5,<3.0.0' \
      'sse-starlette<2.0.0,>=1.6.1' \
      'google-auth>=2.40.0,<3.0.0' \
      'prometheus-client>=0.20.0,<1.0.0' \
      'httpx>=0.28.1,<1.0.0' \
      'PyGithub>=2.0.0,<3.0.0' \
      'aiohttp>=3.13.4,<4.0.0' \
      'jsonpickle>=3.0.0,<4.0.0' \
      'cachetools>=5.0.0,<6.0.0' \
      'redis>=5.0.0,<6.0.0' \
      'anthropic<1.0.0,>=0.49.0' \
      'pyjwt>=2.12.0,<3.0.0' \
      'cryptography>=46.0.7' \
      'pygments>=2.20.0,<3.0.0' \
      'requests>=2.33.0,<3.0.0' \
      'gunicorn[gevent]' \
      'gevent' \
      'pyyaml>=6.0.1,<7.0.0' \
      'botocore>=1.34.0,<2.0.0' \
      'google-cloud-run<1.0.0,>=0.15.0' \
      'google-cloud-compute>=1.45.0,<2.0.0' \
      'google-cloud-artifact-registry>=1.13.0,<2.0.0' \
      'flask>=3.0.0,<4.0.0' \
      'functions-framework>=3.8.0,<4.0.0'

# The vendored sibling repos below (_unified-api-contracts/, _deployment-service/,
# _strategy-service/) are hatchling+hatch-vcs projects (`[tool.hatch.version] source = "vcs"`).
# On the GCP cloudbuild trigger path the `vendor-deps` step clones them --depth 1 and then
# `rm -rf .git`, so setuptools-scm (hatch-vcs delegate) has NO git metadata → "unable to detect
# version" and every `uv pip install` of them fails. SETUPTOOLS_SCM_PRETEND_VERSION pins the
# version for ALL setuptools-scm builds in this stage (PEP440-safe constant; these are installed
# --no-deps so the exact value is metadata-only). cloudbuild passes it via --build-arg; defaulted
# here so AWS CodeBuild / local builds (which don't pass it) also resolve a version. The version
# of THIS project (deployment-api) is never built in the image (deployment_api/ is COPYd, not
# `pip install .`), so this only affects the vendored sibling installs.
ARG SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0
ENV SETUPTOOLS_SCM_PRETEND_VERSION=${SETUPTOOLS_SCM_PRETEND_VERSION}

# Full UAC reinstall from LDR-cloned source. buildspec.aws.yaml pre_build clones
# unified-api-contracts@live-defi-rollout into _unified-api-contracts/ before
# submitting the build context. This ensures registry/, crosscutting/, and all
# other modules (e.g. get_raw_source_data_types in registry/__init__.py) match
# the current LDR state, overriding whatever was baked into the base image.
COPY _unified-api-contracts/ /tmp/_uac/
RUN uv pip install --system --no-cache-dir --no-deps /tmp/_uac && rm -rf /tmp/_uac

# Install deployment-service from the pre-bundled sibling source. The
# tier-3 deploy script rsyncs ../deployment-service/ into ./_deployment-service/
# before submitting the build (Cloud Build context can't reach sibling repos).
COPY _deployment-service/ /tmp/deployment-service/
RUN uv pip install --system --no-cache-dir --no-deps /tmp/deployment-service \
    && rm -rf /tmp/deployment-service

# vm_zombie_watchdog.py is a top-level `scripts/` file in deployment-service, so the
# `--no-deps` wheel install above (package-only) never carries it — it was structurally
# absent from this image regardless of stage (the api-dev stage's `COPY scripts/
# ./scripts/` below copies deployment-api's OWN unrelated scripts/ dir, not this repo's;
# there was never a COPY sourcing _deployment-service/scripts/ anywhere). Every stalled-VM
# auto-kill sweep failed silently in production with "vm_zombie_watchdog unavailable in
# runtime" (heartbeat_stall_watcher_autokill_never_works_in_production_2026_07_27.md).
# `scripts`/`scripts.vm` need no `__init__.py` (PEP 420 namespace packages) — the
# directory landing under /app (already on sys.path via gunicorn's app-loader cwd-insert,
# the same mechanism that resolves `deployment_api.main:app`) is sufficient.
COPY _deployment-service/scripts/vm/vm_zombie_watchdog.py ./scripts/vm/vm_zombie_watchdog.py

# The entire Layer-0 recovery-actuator family (relaunch_consolidator, relaunch_stalled_vm,
# enter_safe_mode, restart_service, etc.) has the SAME structural gap: escalation.py's
# `_ACTUATORS_AVAILABLE` probe (`find_spec("scripts.recovery.relaunch_consolidator")`) was
# never satisfiable in this image either, so every DP_VM_STALL/CONSOLIDATOR_DOWN recovery
# attempt short-circuited to "actuators_not_in_runtime" before instantiating any actuator —
# fixing vm_zombie_watchdog alone restores the kill path but leaves auto-relaunch dead. This
# package is self-contained (only unified_trading_library/unified_api_contracts + stdlib,
# no cross-import on scripts.vm) and already carries its own __init__.py, so the whole
# directory lands as a regular sub-package nested under the scripts/ namespace package.
COPY _deployment-service/scripts/recovery/ ./scripts/recovery/

# Install strategy-service — treasury_routes.py imports strategy_service.position.
# buildspec.aws.yaml clones strategy-service@live-defi-rollout into _strategy-service/.
# sqlalchemy is a direct dep of strategy_service.position.storage.database; install it first
# so --no-deps on strategy-service doesn't leave it missing.
RUN uv pip install --system --no-cache-dir 'sqlalchemy>=2.0.0,<3.0.0'
COPY _strategy-service/ /tmp/strategy-service/
RUN uv pip install --system --no-cache-dir --no-deps /tmp/strategy-service \
    && rm -rf /tmp/strategy-service

COPY deployment_api/ ./deployment_api/
COPY gunicorn.conf.py ./
# Bundled readiness data — populated by cloudbuild before docker build;
# symlinks locally (resolved by docker build context). No-op if absent.
COPY codex-data/ ./codex-data/
COPY pm-plans/ ./pm-plans/
# Operational configs — SSOT is unified-trading-pm/configs/; populated by cloudbuild before docker build
COPY pm-configs/ ./pm-configs/

# UI dist baked from Stage 0 — main.py get_ui_dist_dir() finds <repo>/ui/dist
COPY --from=ui-builder /app/ui/dist ./ui/dist
RUN id -u appuser >/dev/null 2>&1 || useradd --create-home --uid 1000 --shell /bin/bash appuser
RUN chown -R appuser:appuser /app

USER appuser
EXPOSE 8080
ENV PORT=8080
# MEASURED 2026-07-24: never set anywhere in this image. Python block-buffers stdout when it
# isn't a TTY (always true in a container), so log/print output sits in an ~8KB buffer until it
# fills or the process exits cleanly. Combined with this service's observed crash-looping
# (Uncaught signal 6 recurring, one real OOM kill), buffered output was lost before ever
# reaching the pipe Cloud Run captures — zero app-level log lines ever reached Cloud Logging,
# regardless of what the app itself logged. Standard fix: force unbuffered stdio.
ENV PYTHONUNBUFFERED=1
# No OTLP collector sidecar is deployed (single-container Cloud Run), so OpenTelemetry
# would export into the void at localhost:4317 and spam failed-export retries. Disable
# tracing at the image level so every deploy is quiet regardless of runtime env.
# (Runtime override still possible: set TRACING_ENABLED=true once a collector exists.)
ENV TRACING_ENABLED=false
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["gunicorn", "deployment_api.main:app", "-c", "/app/gunicorn.conf.py"]

# Stage for quality gates (test-in-image)
FROM api AS api-dev
USER root
COPY scripts/ ./scripts/
COPY tests/ ./tests/
# Install QG/test extras (deps for the dev/test stage, not production).
RUN uv pip install --system --no-cache-dir \
      'pytest>=9.0.3,<10.0.0' \
      'pytest-cov>=7.0.0,<8.0.0' \
      'pytest-socket>=0.7.0,<1.0.0' \
      'pytest-asyncio>=0.25.0,<2.0.0' \
      'pytest-mock>=3.15.0,<4.0.0' \
      'pytest-timeout>=2.4.0,<3.0.0' \
      'pytest-xdist>=3.6.0,<4.0.0' \
      'ruff==0.15.0' \
      'basedpyright==1.38.2' \
      'pip-audit>=2.7.0,<3.0.0' \
      'bandit>=1.7.0,<2.0.0' \
    && chown -R appuser:appuser /app
USER appuser
