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
ARG BASE_IMAGE_DIGEST=sha256:25a1de57483a5f373077c445b1eaae43b281dbbcd9880536889a5ff8b37d85e9

# ── Stage 0: build deployment-ui static bundle ─────────────────────────
FROM public.ecr.aws/docker/library/node:20-slim@sha256:3d0f05455dea2c82e2f76e7e2543964c30f6b7d673fc1a83286736d44fe4c41c AS ui-builder
WORKDIR /app/ui

# Build context expects ./ui/ to be the deployment-ui repo root (populated
# by cloudbuild git-clone or local symlink). package*.json first for cache.
COPY ui/package*.json ./
RUN npm ci --prefer-offline 2>/dev/null || npm ci

COPY ui/ ./
# Tier-3 shared deploy: bake auth-skip into the SPA so the new Cloud Run
# origin doesn't need to be whitelisted in the workspace's Google OAuth
# client (would otherwise error 401: invalid_client). Vite envs are
# build-time only — must be set BEFORE ``npm run build``.
ENV VITE_SKIP_AUTH=true \
    VITE_MOCK_API=false
RUN npm run build

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
      'flask>=3.0.0,<4.0.0' \
      'functions-framework>=3.8.0,<4.0.0'

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
