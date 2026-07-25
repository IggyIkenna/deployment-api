"""
Centralized Settings Module - Single Source of Truth for Configuration

Uses DeploymentConfig (which extends UnifiedCloudConfig) for type-safe configuration.
All configuration values come from the DeploymentConfig class instead of direct
environment variable calls.

The .env file (sourced by run-api.sh) provides defaults for local development.
Docker/Cloud Run override these via runtime environment variables.
"""

from deployment_api.deployment_api_config import DeploymentApiConfig

# Initialize configuration — uses local DeploymentApiConfig (extends UnifiedCloudConfig).
# Previously imported DeploymentConfig from deployment-service; inlined here to remove
# the cross-service import boundary.
_config = DeploymentApiConfig()

# =============================================================================
# CORE CLOUD CONFIGURATION
# =============================================================================
# gcp_project_id sourced from UnifiedCloudConfig (not environment variable access)
# Named in snake_case; canonical env var is the GCP project env var (handled by UnifiedCloudConfig).
gcp_project_id = _config.gcp_project_id
GCS_REGION = _config.gcs_region
# Expected invoker service account on the OIDC token presented to POST /api/internal/reap-tick
# (Cloud Scheduler -> reap endpoint auth; see deployment_api/routes/_reap_scheduler.py).
REAP_SCHEDULER_INVOKER_SA = _config.reap_scheduler_invoker_sa
# GCP Cloud Build region — PINNED to asia-northeast1, the workspace canonical region where ALL
# Cloud Build triggers + the Artifact Registry live (cloudbuild.yaml is asia-northeast1 fleet-wide;
# operator matched-region decision 2026-05-11). Deliberately NOT GCS_REGION: that config can resolve
# to the us-central1 default, which would query an empty region → the repo-CI Image column reads
# "unknown" for every repo (0 triggers found). Mirrors AWS `_code_builds_aws._AWS_REGION` ("ap-northeast-1").
CLOUD_BUILD_REGION = "asia-northeast1"
STATE_BUCKET = _config.effective_state_bucket
SERVICE_ACCOUNT = _config.service_account_email

# GitHub
GITHUB_ORG = _config.github_org
GITHUB_TOKEN_SA = _config.effective_github_token_sa

# Cloud provider
CLOUD_PROVIDER = _config.cloud_provider

# AWS CodeBuild build-status reader (repo-CI Image column, ?provider=aws). Region matches the GCP
# build region; the role ARN drives keyless GCP->AWS WIF (empty = default boto3 credential chain).
AWS_CODEBUILD_REGION = _config.aws_codebuild_region
AWS_CODEBUILD_READER_ROLE_ARN = _config.aws_codebuild_reader_role_arn

# =============================================================================
# STATE FILE ENVIRONMENT SEPARATION
# =============================================================================
DEPLOYMENT_ENV = _config.deployment_env

# Serial-path toggle for the data-status manifest build (macOS dev hosts — see
# deployment_api_config.data_status_disable_process_pool for the spawn/OOM rationale).
DATA_STATUS_DISABLE_PROCESS_POOL = _config.data_status_disable_process_pool

# Startup index pre-warm target service (empty = off). See
# deployment_api_config.data_status_prewarm_service.
DATA_STATUS_PREWARM_SERVICE = _config.data_status_prewarm_service

# Manifest-status live-build OOM guard (see
# deployment_api.services.data_status.live_build_guard +
# deployment_api.utils.bounded_subprocess) — SSOT for the numbers is the
# field descriptions in deployment_api_config.DeploymentApiConfig.
DATA_STATUS_LIVE_BUILD_MEMORY_BUDGET_BYTES = _config.data_status_live_build_memory_budget_bytes
DATA_STATUS_LIVE_BUILD_CHILD_RLIMIT_BYTES = _config.data_status_live_build_child_rlimit_bytes
DATA_STATUS_LIVE_BUILD_CHILD_TIMEOUT_SECONDS = _config.data_status_live_build_child_timeout_seconds
_DEPLOYMENT_ENV_SHORT_MAP: dict[str, str] = {
    "prod": "prd",
    "prd": "prd",
    "production": "prd",
    "staging": "stg",
    "stg": "stg",
    "development": "dev",
    "dev": "dev",
}
deployment_env_short: str = _DEPLOYMENT_ENV_SHORT_MAP.get((_config.deployment_env or "prod").strip().lower(), "prd")

# =============================================================================
# SERVER CONFIGURATION
# =============================================================================
API_PORT = _config.api_port
WORKERS = _config.workers
# Gunicorn binds to PORT (Cloud Run convention) or API_PORT
PORT = _config.effective_port
FRONTEND_PORT = _config.frontend_port
CORS_ALLOWED_ORIGINS = _config.cors_allowed_origins
CORS_ALLOWED_CLOUD_RUN = _config.cors_allowed_cloud_run
CORS_DEV_ORIGINS = _config.cors_dev_origins

# =============================================================================
# AUTO-SYNC CONFIGURATION
# =============================================================================
AUTO_SYNC_ENABLED = _config.auto_sync_enabled
AUTO_SYNC_INTERVAL_SECONDS = _config.auto_sync_interval_seconds
AUTO_SYNC_INTERVAL_ACTIVE = (
    _config.auto_sync_interval_active
)  # Seconds between syncs when active; 30 to avoid rate limits
AUTO_SYNC_LOCK_TTL_SECONDS = _config.auto_sync_lock_ttl_seconds
AUTO_SYNC_MAX_PARALLEL = (
    _config.auto_sync_max_parallel
)  # Concurrent deployment processors (higher = faster orphan kill)
# Fire-and-forget orphan VM termination (parallel, non-blocking)
ORPHAN_DELETE_MAX_PARALLEL = (
    _config.orphan_delete_max_parallel
)  # Max concurrent delete requests (keeps deletes within write quota)
ORPHAN_DELETE_RETRY_SECONDS = (
    _config.orphan_delete_retry_seconds
)  # Retry delete if VM still RUNNING after this many seconds
# Run orphan cleanup for deployments marked completed in last N minutes (catches missed deletes)
ORPHAN_CLEANUP_RECENTLY_COMPLETED_MINUTES = _config.orphan_cleanup_recently_completed_minutes

# GCP Compute Engine write quota (insert + delete); fixed pool Option A
WRITE_QUOTA_PER_MINUTE = _config.write_quota_per_minute  # GCP default ~6k writes/min
WRITE_QUOTA_BUFFER = (
    _config.write_quota_buffer
)  # Reserve for other workloads; effective = WRITE_QUOTA_PER_MINUTE - WRITE_QUOTA_BUFFER

# =============================================================================
# AUTO-SCHEDULER CONFIGURATION
# =============================================================================
DEFAULT_MAX_CONCURRENT = _config.default_max_concurrent
MAX_CONCURRENT_HARD_LIMIT = _config.max_concurrent_hard_limit
AUTO_SCHEDULER_MAX_LAUNCH_PER_TICK = _config.auto_scheduler_max_launch_per_tick
AUTO_SCHEDULER_MAX_RELEASES_PER_TICK = _config.auto_scheduler_max_releases_per_tick
AUTO_SCHEDULER_BATCH_SIZE = _config.auto_scheduler_batch_size
AUTO_SCHEDULER_INTER_BATCH_DELAY = _config.auto_scheduler_inter_batch_delay
AUTO_SCHEDULER_DELETE_BATCH_SIZE = _config.auto_scheduler_delete_batch_size
AUTO_SCHEDULER_DELETE_BATCH_DELAY_SECONDS = _config.auto_scheduler_delete_batch_delay_seconds
AUTO_SCHEDULER_PARALLEL_WORKERS = _config.auto_scheduler_parallel_workers
AUTO_SCHEDULER_VM_RATE_LIMIT = _config.auto_scheduler_vm_rate_limit
STUCK_SHARD_GRACE_SECONDS = _config.stuck_shard_grace_seconds
# VM health check: terminate if no startup signal after this many seconds
VM_STARTUP_TIMEOUT_SECONDS = _config.vm_startup_timeout_seconds  # 5 minutes
# OOM detection: terminate if this many OOM messages in serial logs
OOM_KILL_THRESHOLD = _config.oom_kill_threshold
# State TTL: cleanup old deployment state files (keep last N hours)
STATE_TTL_HOURS = _config.state_ttl_hours  # Keep last 48 hours

# =============================================================================
# VM ORCHESTRATION
# =============================================================================
VM_LAUNCH_MINI_BATCH_SIZE = _config.vm_launch_mini_batch_size
VM_LAUNCH_MINI_BATCH_DELAY_SECONDS = _config.vm_launch_mini_batch_delay_seconds
UNKNOWN_STATUS_MAX_POLLS = _config.unknown_status_max_polls

# =============================================================================
# PERFORMANCE TUNING
# =============================================================================
GCS_POOL_SIZE = _config.gcs_pool_size
COMPUTE_POOL_SIZE = _config.compute_pool_size
COMPUTE_POOL_MAXSIZE = _config.compute_pool_maxsize

# =============================================================================
# CACHE
# =============================================================================
REDIS_URL = _config.redis_url
DATA_STATUS_CACHE_TTL_SECONDS = _config.data_status_cache_ttl_seconds

# =============================================================================
# QUOTA BROKER
# =============================================================================
QUOTA_BROKER_URL = _config.quota_broker_url.rstrip("/") if _config.quota_broker_url else ""
QUOTA_BROKER_AUTH_MODE = _config.quota_broker_auth_mode
QUOTA_BROKER_TIMEOUT_SECONDS = _config.quota_broker_timeout_seconds
BROKER_MAX_WAIT_SECONDS = _config.broker_max_wait_seconds

# =============================================================================
# WORKSPACE CONFIGURATION
# =============================================================================
WORKSPACE_ROOT = _config.workspace_root

# =============================================================================
# GCS STORE BUCKETS
# =============================================================================
EXECUTION_STORE_BUCKET = _config.effective_execution_store_bucket
STRATEGY_STORE_CEFI_BUCKET = _config.effective_strategy_store_cefi_bucket
STRATEGY_STORE_TRADFI_BUCKET = _config.effective_strategy_store_tradfi_bucket
STRATEGY_STORE_DEFI_BUCKET = _config.effective_strategy_store_defi_bucket
ML_CONFIGS_STORE_BUCKET = _config.effective_ml_configs_store_bucket

# =============================================================================
# TESTING
# =============================================================================
CLOUD_MOCK_MODE = _config.is_mock_mode()

# =============================================================================
# DEPLOYMENT-SERVICE HTTP CLIENT CONFIGURATION
# =============================================================================
DEPLOYMENT_SERVICE_URL = _config.deployment_service_url.rstrip("/")
DEPLOYMENT_SERVICE_TIMEOUT_SECONDS = _config.deployment_service_timeout_seconds

# =============================================================================
# SINGLE-REGION / ZONE FAILOVER
# =============================================================================
ENFORCE_SINGLE_REGION = _config.enforce_single_region
WARN_CROSS_REGION_EGRESS = _config.warn_cross_region_egress
# Single region for VM/Cloud Run (zone failover within region only)
ALL_FAILOVER_REGIONS = _config.all_failover_regions
