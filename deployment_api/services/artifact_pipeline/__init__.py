"""Artifact pipeline observability — build → artifact → deploy lineage for /ops/artifacts.

Reads the live cloud APIs (GCP Cloud Build + Artifact Registry + Cloud Run revisions; AWS CodeBuild +
ECR + App Runner/ECS; GCS tarball manifests), normalizes them into one shape, and serves five views:
running / deploys / builds / images / health. Expensive multi-cloud scans are served from a periodic
GCS parquet snapshot + a small TTL cache (the cost-observability pattern), so the page is cheap and
OOM-safe. Unknowns render as explicit "unknown + why", never a fabricated green.

SSOT: plans/active/artifact_pipeline_observability_2026_07_17.md
"""

from deployment_api.services.artifact_pipeline.models import (
    BuildsResponse,
    DeploysResponse,
    HealthResponse,
    ImagesResponse,
    RunningResponse,
)
from deployment_api.services.artifact_pipeline.service import ArtifactPipelineService

__all__ = [
    "ArtifactPipelineService",
    "BuildsResponse",
    "DeploysResponse",
    "HealthResponse",
    "ImagesResponse",
    "RunningResponse",
]
