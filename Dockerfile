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

# ── Stage 0: build deployment-ui static bundle ─────────────────────────
FROM node:20-slim AS ui-builder
WORKDIR /app/ui

# Build context expects ./ui/ to be the deployment-ui repo root (populated
# by cloudbuild git-clone or local symlink). package*.json first for cache.
COPY ui/package*.json ./
RUN npm ci --prefer-offline 2>/dev/null || npm ci

COPY ui/ ./
RUN npm run build

# ── Stage 1: Python API base ────────────────────────────────────────────
FROM --platform=linux/amd64 asia-northeast1-docker.pkg.dev/${PROJECT_ID}/unified-trading-library/unified-trading-library:latest AS base

FROM base AS api
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends ripgrep tini \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --system \
    && uv pip install --system --no-cache-dir gunicorn[gevent] gevent

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
RUN uv sync --frozen --no-dev --system && chown -R appuser:appuser /app
USER appuser
