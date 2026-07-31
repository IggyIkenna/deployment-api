# Deployment API — Deployment Guide

## Overview

Deployment API is the backend for the deployment UI. Deploy as Cloud Run service.

## Prerequisites

- GCP project, deployment-service as dependency
- GCP_PROJECT_ID, STATE_BUCKET, REDIS_URL (if using cache)

## Deployment Steps

Images are published to Artifact Registry (Container Registry / `gcr.io/` is deprecated). The canonical image path is
`asia-northeast1-docker.pkg.dev/{project_id}/{repository}/deployment-api:{tag}`.

```bash
gcloud builds submit \
  --tag asia-northeast1-docker.pkg.dev/{project_id}/{repository}/deployment-api:latest
gcloud run deploy deployment-api \
  --image asia-northeast1-docker.pkg.dev/{project_id}/{repository}/deployment-api:latest \
  --region {region} \
  --set-env-vars GCP_PROJECT_ID={project_id}
```

## Health Check

`GET /health`
