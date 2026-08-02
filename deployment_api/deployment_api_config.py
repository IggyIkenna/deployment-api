"""
Deployment API Configuration — extends UnifiedCloudConfig with deployment-specific settings.

This class was previously imported as DeploymentConfig from deployment-service.
It is inlined here to remove the cross-service import boundary. deployment-api must not
import deployment-service as a Python package; interaction is via messaging/APIs/storage.

All fields read from the same environment variables as before. No behaviour change.
"""

from typing import cast

from pydantic import AliasChoices, Field
from unified_trading_library import UnifiedCloudConfig, resolve_bucket_name
from unified_trading_library.cloud_interface import Cloud  # noqa: qg-deep-import — Cloud not yet on UTL root facade


class DeploymentApiConfig(UnifiedCloudConfig):
    """
    Configuration for deployment-api.

    Provides deployment-specific configuration on top of UnifiedCloudConfig.
    """

    # =========================================================================
    # DEPLOYMENT-SPECIFIC CONFIGURATION
    # =========================================================================

    deployment_env: str = Field(
        default="development",
        validation_alias=AliasChoices("DEPLOYMENT_ENV"),
        description="Deployment environment (development, staging, production)",
    )

    state_bucket: str = Field(
        default="",
        validation_alias=AliasChoices("STATE_BUCKET"),
        description="GCS bucket for deployment state storage",
    )

    # =========================================================================
    # ALERTS-LEDGER CONFIGURATION
    # =========================================================================

    alerting_service_source_enabled: bool = Field(
        default=True,
        validation_alias=AliasChoices("ALERTING_SERVICE_SOURCE_ENABLED"),
        description=(
            "Kill switch for the alerting-service source plane in /api/alerts "
            "(see alerts_endpoint_per_object_gcs_read_performance_2026_07_23.md — its bucket writes "
            "one object per alert event, ~20-24k objects/day, and the reader lists+downloads each one "
            "sequentially). Default True (unchanged behavior). Set False to skip this plane while the "
            "underlying per-object read pattern is unfixed, without falling back to mock data."
        ),
    )

    data_status_disable_process_pool: bool = Field(
        default=False,
        validation_alias=AliasChoices("DATA_STATUS_DISABLE_PROCESS_POOL"),
        description=(
            "Force the data-status manifest build off the per-category fork ProcessPool. "
            "Required on macOS dev hosts: the pool forks after grpc/GCS init, which dies on "
            "macOS (BrokenProcessPool; diagnosed 2026-06-12). With the pool off, the build "
            "falls back to a THREAD pool (overlaps the I/O-bound index loads; cell-grid "
            "compute stays GIL-serial) — not pure serial — so it stays fast without forking."
        ),
    )

    data_status_prewarm_service: str = Field(
        default="",
        validation_alias=AliasChoices("DATA_STATUS_PREWARM_SERVICE"),
        description=(
            "If set to a service name (e.g. 'market-tick-data-service'), the API warms that "
            "service's manifest indexes into _INDEX_CACHE at startup via a tiny background "
            "7-day query — so the first real Data Status query hits warm cache (~10s) instead "
            "of the cold transpacific GCS load (~50s/asset-group). Empty = no pre-warm "
            "(default; avoids a boot-time memory spike on tight dev hosts)."
        ),
    )

    data_status_live_build_memory_budget_bytes: int = Field(
        default=768 * 1024**2,  # 768 MiB
        validation_alias=AliasChoices("DATA_STATUS_LIVE_BUILD_MEMORY_BUDGET_BYTES"),
        description=(
            "Pre-flight refusal ceiling (see deployment_api.services.data_status."
            "live_build_guard.estimate_live_build_bytes) for the manifest-status on-demand "
            "live cell-grid build. If the cheap request-shape estimate exceeds this, the live "
            "build is refused outright (serve a stale rollup, or a structured "
            "narrow-your-range error) rather than attempted. 768 MiB on a 4 GiB Cloud Run "
            "container: ~2.5 GiB baseline + other concurrent requests leaves headroom for "
            "roughly one build at this ceiling plus margin below the child rlimit "
            "(data_status_live_build_child_rlimit_bytes) — deliberately BELOW that rlimit so "
            "a build the pre-flight estimate underestimates trips the rlimit before it would "
            "have threatened the container regardless. Calibrated against measured OOM "
            "incidents (2026-07-13/14): 18 GB (instruments-service full-history), 81 GB "
            "(market-tick-data-service full-history), 56 GB (market-data-processing-service, "
            "just 3 months) for a single uncapped request."
        ),
    )

    data_status_live_build_child_rlimit_bytes: int = Field(
        default=1024 * 1024**2,  # 1 GiB
        validation_alias=AliasChoices("DATA_STATUS_LIVE_BUILD_CHILD_RLIMIT_BYTES"),
        description=(
            "RLIMIT_AS ceiling for the spawned child process that runs the manifest-status "
            "live cell-grid build (defense-in-depth layer 2 — see "
            "deployment_api.utils.bounded_subprocess.run_bounded). Applies even to builds "
            "that passed the pre-flight memory-budget estimate, since the estimate is a "
            "cheap heuristic, not a measurement: an underestimate raises a catchable "
            "MemoryError in the throwaway child instead of the platform OOM-killer taking "
            "down the whole container. Set slightly ABOVE the pre-flight budget (1 GiB vs "
            "768 MiB) so passing the estimate doesn't immediately trip the hard ceiling; "
            "still comfortably under the 4 GiB container limit alongside the ~2.5 GiB "
            "baseline + other concurrent requests."
        ),
    )

    data_status_live_build_child_timeout_seconds: float = Field(
        default=120.0,
        validation_alias=AliasChoices("DATA_STATUS_LIVE_BUILD_CHILD_TIMEOUT_SECONDS"),
        description=(
            "Wall-clock backstop for the manifest-status live-build child process (in "
            "addition to the RLIMIT_AS ceiling) — a hung (not necessarily OOMing) child "
            "must not block the request forever."
        ),
    )

    service_account_email: str = Field(
        default="",
        validation_alias=AliasChoices("SERVICE_ACCOUNT", "SERVICE_ACCOUNT_EMAIL"),
        description="Service account email for deployments",
    )

    # Cloud Scheduler -> POST /api/internal/reap-tick OIDC invoker identity (config, not a secret —
    # this is checked AGAINST the caller's Google-signed OIDC token, not used to authenticate
    # outbound; empty = the endpoint refuses every caller). See
    # deployment_registry_reaper_not_draining_stale_entries_2026_07_24.md.
    reap_scheduler_invoker_sa: str = Field(
        default="",
        validation_alias=AliasChoices("REAP_SCHEDULER_INVOKER_SA"),
        description="Expected SA email on the OIDC token Cloud Scheduler presents to /api/internal/reap-tick",
    )

    # AWS CodeBuild build-status reader (repo-CI Image column, ?provider=aws). Config, not secrets.
    aws_codebuild_region: str = Field(
        default="ap-northeast-1",
        validation_alias=AliasChoices("AWS_CODEBUILD_REGION"),
        description=(
            "AWS region the CodeBuild build-status reader queries for the repo-CI Image column "
            "when provider=aws. Matches the GCP asia-northeast1 build region."
        ),
    )
    aws_codebuild_reader_role_arn: str = Field(
        default="",
        validation_alias=AliasChoices("AWS_CODEBUILD_READER_ROLE_ARN"),
        description=(
            "ARN of the keyless GCP->AWS Workload-Identity-Federation role the Cloud Run service "
            "account assumes (AssumeRoleWithWebIdentity) for READ-ONLY CodeBuild access. The role's "
            "trust policy is locked to this SA's OIDC subject - no static AWS key anywhere. Empty = "
            "use the default boto3 credential chain (native-AWS task role / local AWS profile). "
            "Config, not a secret."
        ),
    )

    # =========================================================================
    # COST OBSERVABILITY — billing export sources (/ops/costs)
    # SSOT: codex/05-infrastructure/billing-cost-observability.md. Config, not secrets.
    # =========================================================================
    gcp_billing_dataset: str = Field(
        default="billing_export",
        validation_alias=AliasChoices("GCP_BILLING_DATASET"),
        description="BigQuery dataset holding the GCP billing export tables.",
    )
    gcp_billing_resource_table: str = Field(
        default="gcp_billing_export_resource_v1_016B25_109840_AF2ACB",
        validation_alias=AliasChoices("GCP_BILLING_RESOURCE_TABLE"),
        description="Resource-level GCP billing export table (per-VM / per-bucket rows).",
    )
    aws_cur_database: str = Field(
        default="aws_billing",
        validation_alias=AliasChoices("AWS_CUR_DATABASE"),
        description="Glue database for the AWS CUR→Athena export.",
    )
    aws_cur_table: str = Field(
        default="cur_uts_cost_usage",
        validation_alias=AliasChoices("AWS_CUR_TABLE"),
        description="Athena table (crawler-built) over the AWS Cost and Usage Report.",
    )
    aws_cur_region: str = Field(
        default="us-east-1",
        validation_alias=AliasChoices("AWS_CUR_REGION"),
        description="Region the CUR + Athena workgroup live in (us-east-1, not the app default).",
    )
    aws_athena_output_bucket: str = Field(
        default="uts-billing-cur-427895769566",
        validation_alias=AliasChoices("AWS_ATHENA_OUTPUT_BUCKET"),
        description="S3 bucket for Athena query results (the CUR delivery bucket's athena-results/ prefix).",
    )
    aws_athena_reader_role_arn: str = Field(
        default="",
        validation_alias=AliasChoices("AWS_ATHENA_READER_ROLE_ARN"),
        description=(
            "ARN of the keyless GCP->AWS Workload-Identity-Federation role the Cloud Run service "
            "account assumes (AssumeRoleWithWebIdentity) for READ-ONLY Athena/Glue access — the AWS "
            "CUR query path behind the cost-observability AWS tab. Same pattern (+ same trust-policy "
            "shape) as ``aws_codebuild_reader_role_arn`` above, just a distinct role scoped to "
            "Athena/Glue instead of CodeBuild. Empty = the default boto3 credential chain, which the "
            "deployed Cloud Run container has NO ambient AWS credentials for (verified live — its "
            "only env vars are GCP-side), so the AWS cost tab silently returns nothing there until an "
            "operator provisions the AWS-side role + sets this. Config, not a secret."
        ),
    )
    github_billing_account: str = Field(
        default="IggyIkenna",
        validation_alias=AliasChoices("GITHUB_BILLING_ACCOUNT"),
        description="GitHub user/org whose Enhanced Billing usage report backs the GitHub cost tab.",
    )
    github_billing_account_type: str = Field(
        default="user",
        validation_alias=AliasChoices("GITHUB_BILLING_ACCOUNT_TYPE"),
        description="'user' or 'org' — selects the /users/{} vs /organizations/{} billing endpoint.",
    )
    github_billing_secret: str = Field(
        default="github-billing-token",
        validation_alias=AliasChoices("GITHUB_BILLING_SECRET"),
        description="Secret Manager key for the GitHub Plan-scoped billing token (falls back to GH_PAT).",
    )

    # =========================================================================
    # DEPLOYMENT CONCURRENCY LIMITS
    # =========================================================================

    default_max_concurrent: int = Field(
        default=2000,
        validation_alias=AliasChoices("DEFAULT_MAX_CONCURRENT"),
        description="Default maximum concurrent deployments",
    )

    max_concurrent_hard_limit: int = Field(
        default=2500,
        validation_alias=AliasChoices("MAX_CONCURRENT_HARD_LIMIT"),
        description="Hard limit for maximum concurrent deployments",
    )

    # =========================================================================
    # QUOTA BROKER CONFIGURATION
    # =========================================================================

    quota_broker_url: str = Field(
        default="",
        validation_alias=AliasChoices("QUOTA_BROKER_URL"),
        description="URL for quota broker service",
    )

    quota_broker_auth_mode: str = Field(
        default="iam",
        validation_alias=AliasChoices("QUOTA_BROKER_AUTH_MODE"),
        description="Authentication mode for quota broker (iam or none)",
    )

    quota_broker_timeout_seconds: float = Field(
        default=10.0,
        validation_alias=AliasChoices("QUOTA_BROKER_TIMEOUT_SECONDS"),
        description="Timeout for quota broker requests in seconds",
    )

    # =========================================================================
    # SERVER CONFIGURATION
    # =========================================================================

    api_port: str = Field(
        default="8000",
        validation_alias=AliasChoices("API_PORT"),
        description="API server port",
    )

    workers: int = Field(
        default=4,
        validation_alias=AliasChoices("WORKERS"),
        description="Number of worker processes",
    )

    port: str = Field(
        default="",
        validation_alias=AliasChoices("PORT"),
        description="Port for Gunicorn binding (Cloud Run convention)",
    )

    frontend_port: str = Field(
        default="5173",
        validation_alias=AliasChoices("FRONTEND_PORT"),
        description="Frontend dev server port (for CORS dev origins)",
    )

    cors_allowed_origins: str = Field(
        default="",
        validation_alias=AliasChoices("CORS_ALLOWED_ORIGINS"),
        description="Comma-separated list of allowed CORS origins in production",
    )

    cors_allowed_cloud_run: str = Field(
        default="",
        validation_alias=AliasChoices("CORS_ALLOWED_CLOUD_RUN"),
        description="Comma-separated Cloud Run service names allowed via CORS regex",
    )

    cors_dev_origins: str = Field(
        default="http://localhost:3000,http://localhost:5174,http://127.0.0.1:5174,http://localhost:5183,http://127.0.0.1:5183,http://localhost:8080",
        validation_alias=AliasChoices("CORS_DEV_ORIGINS"),
        description="Comma-separated static localhost origins allowed in development mode",
    )

    # =========================================================================
    # AUTO-SYNC CONFIGURATION
    # =========================================================================

    auto_sync_enabled: bool = Field(
        default=True,
        validation_alias=AliasChoices("AUTO_SYNC_ENABLED"),
        description="Enable automatic synchronization",
    )

    auto_sync_interval_seconds: int = Field(
        default=60,
        validation_alias=AliasChoices("AUTO_SYNC_INTERVAL_SECONDS"),
        description="Interval between auto-sync operations in seconds",
    )

    auto_sync_interval_active: int = Field(
        default=30,
        validation_alias=AliasChoices("AUTO_SYNC_INTERVAL_ACTIVE"),
        description="Seconds between syncs when active; 30 to avoid rate limits",
    )

    auto_sync_lock_ttl_seconds: int = Field(
        default=90,
        validation_alias=AliasChoices("AUTO_SYNC_LOCK_TTL_SECONDS"),
        description="Lock TTL for auto-sync operations in seconds",
    )

    auto_sync_max_parallel: int = Field(
        default=10,
        validation_alias=AliasChoices("AUTO_SYNC_MAX_PARALLEL"),
        description="Concurrent deployment processors (higher = faster orphan kill)",
    )

    orphan_delete_max_parallel: int = Field(
        default=500,
        validation_alias=AliasChoices("ORPHAN_DELETE_MAX_PARALLEL"),
        description="Max concurrent delete requests (keeps deletes within write quota)",
    )

    orphan_delete_retry_seconds: int = Field(
        default=30,
        validation_alias=AliasChoices("ORPHAN_DELETE_RETRY_SECONDS"),
        description="Retry delete if VM still RUNNING after this many seconds",
    )

    orphan_cleanup_recently_completed_minutes: int = Field(
        default=30,
        validation_alias=AliasChoices("ORPHAN_CLEANUP_RECENTLY_COMPLETED_MINUTES"),
        description="Run orphan cleanup for deployments marked completed in last N minutes",
    )

    write_quota_per_minute: int = Field(
        default=6000,
        validation_alias=AliasChoices("WRITE_QUOTA_PER_MINUTE"),
        description="GCP Compute Engine write quota (insert + delete)",
    )

    write_quota_buffer: int = Field(
        default=1000,
        validation_alias=AliasChoices("WRITE_QUOTA_BUFFER"),
        description="Reserve for other workloads",
    )

    # =========================================================================
    # AUTO-SCHEDULER CONFIGURATION
    # =========================================================================

    auto_scheduler_max_launch_per_tick: int = Field(
        default=2000,
        validation_alias=AliasChoices("AUTO_SCHEDULER_MAX_LAUNCH_PER_TICK"),
        description="Maximum launches per scheduler tick",
    )

    auto_scheduler_max_releases_per_tick: int = Field(
        default=200,
        validation_alias=AliasChoices("AUTO_SCHEDULER_MAX_RELEASES_PER_TICK"),
        description="Maximum releases per scheduler tick",
    )

    auto_scheduler_batch_size: int = Field(
        default=200,
        validation_alias=AliasChoices("AUTO_SCHEDULER_BATCH_SIZE"),
        description="Auto-scheduler batch size",
    )

    auto_scheduler_inter_batch_delay: float = Field(
        default=1.0,
        validation_alias=AliasChoices("AUTO_SCHEDULER_INTER_BATCH_DELAY"),
        description="Delay between auto-scheduler batches in seconds",
    )

    auto_scheduler_delete_batch_size: int = Field(
        default=200,
        validation_alias=AliasChoices("AUTO_SCHEDULER_DELETE_BATCH_SIZE"),
        description="Auto-scheduler delete batch size",
    )

    auto_scheduler_delete_batch_delay_seconds: int = Field(
        default=45,
        validation_alias=AliasChoices("AUTO_SCHEDULER_DELETE_BATCH_DELAY_SECONDS"),
        description="Delay between delete batches in seconds",
    )

    auto_scheduler_parallel_workers: int = Field(
        default=56,
        validation_alias=AliasChoices("AUTO_SCHEDULER_PARALLEL_WORKERS"),
        description="Number of parallel auto-scheduler workers",
    )

    auto_scheduler_vm_rate_limit: float = Field(
        default=10.0,
        validation_alias=AliasChoices("AUTO_SCHEDULER_VM_RATE_LIMIT"),
        description="VM rate limit for auto-scheduler",
    )

    stuck_shard_grace_seconds: int = Field(
        default=600,
        validation_alias=AliasChoices("STUCK_SHARD_GRACE_SECONDS"),
        description="Grace period for stuck shards in seconds",
    )

    vm_startup_timeout_seconds: int = Field(
        default=300,
        validation_alias=AliasChoices("VM_STARTUP_TIMEOUT_SECONDS"),
        description="VM health check: terminate if no startup signal after this many seconds",
    )

    oom_kill_threshold: int = Field(
        default=5,
        validation_alias=AliasChoices("OOM_KILL_THRESHOLD"),
        description="OOM detection: terminate if this many OOM messages in serial logs",
    )

    state_ttl_hours: int = Field(
        default=48,
        validation_alias=AliasChoices("STATE_TTL_HOURS"),
        description="State TTL: cleanup old deployment state files (keep last N hours)",
    )

    # =========================================================================
    # VM ORCHESTRATION
    # =========================================================================

    vm_launch_mini_batch_size: int = Field(
        default=200,
        validation_alias=AliasChoices("VM_LAUNCH_MINI_BATCH_SIZE"),
        description="VM launch mini-batch size",
    )

    vm_launch_mini_batch_delay_seconds: float = Field(
        default=3.0,
        validation_alias=AliasChoices("VM_LAUNCH_MINI_BATCH_DELAY_SECONDS"),
        description="Delay between VM launch mini-batches in seconds",
    )

    unknown_status_max_polls: int = Field(
        default=10,
        validation_alias=AliasChoices("UNKNOWN_STATUS_MAX_POLLS"),
        description="Maximum polls for unknown status VMs",
    )

    # =========================================================================
    # PERFORMANCE TUNING
    # =========================================================================

    gcs_pool_size: int = Field(
        default=200,
        validation_alias=AliasChoices("GCS_POOL_SIZE"),
        description="GCS connection pool size",
    )

    compute_pool_size: int = Field(
        default=200,
        validation_alias=AliasChoices("COMPUTE_POOL_SIZE"),
        description="Compute engine connection pool size",
    )

    compute_pool_maxsize: int = Field(
        default=200,
        validation_alias=AliasChoices("COMPUTE_POOL_MAXSIZE"),
        description="Compute engine connection pool max size",
    )

    # =========================================================================
    # CACHE CONFIGURATION
    # =========================================================================

    redis_url: str = Field(
        default="redis://localhost:6379/0",
        validation_alias=AliasChoices("REDIS_URL"),
        description="Redis server URL",
    )

    data_status_cache_ttl_seconds: int = Field(
        default=1800,
        validation_alias=AliasChoices("DATA_STATUS_CACHE_TTL_SECONDS"),
        description="Data status cache TTL in seconds",
    )

    # =========================================================================
    # GITHUB CONFIGURATION
    # =========================================================================

    github_org: str = Field(
        default="IggyIkenna",
        validation_alias=AliasChoices("GITHUB_ORG"),
        description="GitHub organization",
    )

    github_token_sa: str = Field(
        default="",
        validation_alias=AliasChoices("GITHUB_TOKEN_SA"),
        description="GitHub token service account",
    )

    # =========================================================================
    # DEPLOYMENT FLAGS
    # =========================================================================

    enforce_single_region: bool = Field(
        default=True,
        validation_alias=AliasChoices("ENFORCE_SINGLE_REGION"),
        description="Enforce single region deployment",
    )

    warn_cross_region_egress: bool = Field(
        default=True,
        validation_alias=AliasChoices("WARN_CROSS_REGION_EGRESS"),
        description="Warn about cross-region egress",
    )

    broker_max_wait_seconds: float = Field(
        default=900.0,
        validation_alias=AliasChoices("BROKER_MAX_WAIT_SECONDS"),
        description="Maximum wait time for broker requests in seconds",
    )

    workspace_root: str = Field(
        default="",
        validation_alias=AliasChoices("WORKSPACE_ROOT"),
        description="Absolute path to the workspace root",
    )

    cloud_mock_mode: bool = Field(  # DEPRECATED: use is_mock_mode() instead
        default=False,
        validation_alias=AliasChoices("CLOUD_MOCK_MODE"),
        description=("DEPRECATED: Use is_mock_mode() instead. Legacy field kept for env var binding."),
    )

    # =========================================================================
    # GCS STORE BUCKET CONFIGURATION
    # =========================================================================

    execution_store_bucket: str = Field(
        default="",
        validation_alias=AliasChoices("EXECUTION_STORE_BUCKET"),
        description="GCS bucket name for execution store configs (without gs:// prefix). "
        "execution-store is a FLAT env-tiered yaml kind post Fold C "
        "(bucket_fold_execution_strategy_2026_07_17.md): resolve_bucket_name(kind="
        "'execution-store') folds to execution-store-{env}-{pid} and IGNORES asset_group; "
        "asset_group is now an object-key prefix ('{ag}/') within that shared bucket. "
        "Defaults (when empty) to that folded bucket — see effective_execution_store_bucket.",
    )

    strategy_store_cefi_bucket: str = Field(
        default="",
        validation_alias=AliasChoices("STRATEGY_STORE_CEFI_BUCKET"),
        description="GCS bucket name for strategy store CeFi configs (without gs:// prefix). "
        "strategy-store is a FLAT yaml kind (D6 Phase 4 operator decision 2026-05-20); "
        "defaults to the unified flat strategy-store bucket (resolve_bucket_name "
        "kind='strategy-store') when empty — same bucket as tradfi/defi.",
    )

    strategy_store_tradfi_bucket: str = Field(
        default="",
        validation_alias=AliasChoices("STRATEGY_STORE_TRADFI_BUCKET"),
        description="GCS bucket name for strategy store TradFi configs (without gs:// prefix). "
        "Defaults to the unified flat strategy-store bucket when empty — same bucket "
        "as cefi/defi (see strategy_store_cefi_bucket).",
    )

    strategy_store_defi_bucket: str = Field(
        default="",
        validation_alias=AliasChoices("STRATEGY_STORE_DEFI_BUCKET"),
        description="GCS bucket name for strategy store DeFi configs (without gs:// prefix). "
        "Defaults to the unified flat strategy-store bucket when empty — same bucket "
        "as cefi/tradfi (see strategy_store_cefi_bucket).",
    )

    ml_configs_store_bucket: str = Field(
        default="",
        validation_alias=AliasChoices("ML_CONFIGS_STORE_BUCKET"),
        description="GCS bucket name for ML configs store (without gs:// prefix). "
        "Defaults (when empty) to the folded ml-store bucket — ml FOLD B "
        "(bucket_fold_ml_2026_07_17.md): resolve_bucket_name(kind='ml-configs-store') "
        "folds through _KIND_ALIASES to ml-store-{env}-{pid}; the configs are namespaced "
        "under the 'configs/' object-key prefix within that shared bucket.",
    )

    # =========================================================================
    # DEPLOYMENT-SERVICE HTTP CLIENT CONFIGURATION
    # =========================================================================

    deployment_service_url: str = Field(
        default="http://localhost:9000",
        validation_alias=AliasChoices("DEPLOYMENT_SERVICE_URL"),
        description="Base URL for the deployment-service HTTP API",
    )

    execution_service_url: str = Field(
        default="http://localhost:8018",
        validation_alias=AliasChoices("EXECUTION_SERVICE_URL"),
        description=(
            "Base URL for the execution-service HTTP API. Used by the manual-pending "
            "approve/reject endpoints to forward operator decisions to the execution queue."
        ),
    )

    deployment_service_timeout_seconds: float = Field(
        default=300.0,
        validation_alias=AliasChoices("DEPLOYMENT_SERVICE_TIMEOUT_SECONDS"),
        description="Timeout in seconds for deployment-service HTTP calls",
    )

    # =========================================================================
    # LIVE-PIPELINE HEALTH-API REGISTRY (per-service URL → /health endpoint)
    # =========================================================================
    #
    # Per ``live_pipeline_mtds_mdps_features_2026_05_08.md`` Phase 11.1.
    # The /api/data-status/live endpoint joins per-shard health from
    # each consumer service's :func:`make_health_router(data_freshness=)`
    # callback (Phase 8). Without a URL registry the endpoint serves
    # only the manifest-derived staleness; with it serves precise
    # ``last_event_age_seconds`` from each service's
    # :class:`StreamingHealthSnapshot`.
    #
    # JSON-shape: ``{"<service-name>": "http://host:port", ...}``. Keys
    # are the lowercase service names per workspace convention
    # (``market-tick-data-service`` / ``market-data-processing-service`` /
    # ``features-service``). Empty dict (default) → no HTTP join; the
    # endpoint serves manifest-only data.

    live_pipeline_service_urls: dict[str, str] = Field(
        default_factory=dict,
        validation_alias=AliasChoices("LIVE_PIPELINE_SERVICE_URLS"),
        description=(
            "Per-service base URLs for the live-pipeline Health-API HTTP "
            "join. Each URL must serve GET /health returning JSON with a "
            "`data_freshness` field per the UTL StreamingHealthSnapshot "
            "contract. Empty dict (default) → manifest-only fallback."
        ),
    )

    live_pipeline_health_timeout_seconds: float = Field(
        default=2.0,
        validation_alias=AliasChoices("LIVE_PIPELINE_HEALTH_TIMEOUT_SECONDS"),
        description=(
            "Timeout in seconds for each per-service Health-API call. "
            "Short by design: the /api/data-status/live endpoint must "
            "stay responsive; a slow service → manifest-only fallback "
            "for that service's shards."
        ),
    )

    # =========================================================================
    # PIPELINE UAT COMMENTARY
    # =========================================================================

    pipeline_uat_commentary_enabled: bool = Field(
        default=False,
        validation_alias=AliasChoices("PIPELINE_UAT_COMMENTARY_ENABLED"),
        description="Enable pipeline UAT commentary via Anthropic API",
    )

    anthropic_api_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices("ANTHROPIC_API_KEY"),
        description="Anthropic API key for pipeline UAT commentary (None = disabled)",
    )

    pipeline_uat_model: str = Field(
        default="claude-haiku-4-5-20251001",
        validation_alias=AliasChoices("PIPELINE_UAT_MODEL"),
        description="Anthropic model ID for pipeline UAT commentary",
    )

    pipeline_uat_max_tokens: int = Field(
        default=800,
        validation_alias=AliasChoices("PIPELINE_UAT_MAX_TOKENS"),
        description="Max tokens for pipeline UAT Anthropic API response",
    )

    pipeline_uat_llm_base_url: str | None = Field(
        default=None,
        validation_alias=AliasChoices("PIPELINE_UAT_LLM_BASE_URL"),
        description=(
            "Optional Anthropic-compatible base URL override for pipeline UAT commentary "
            "(e.g. a local OmniRoute gateway). None = call Anthropic directly (default, unchanged behavior)."
        ),
    )

    # =========================================================================
    # RUN.LOG TAIL ENDPOINT (WS-4)
    # =========================================================================

    run_log_tail_max_bytes: int = Field(
        default=256 * 1024,  # 256 KiB
        validation_alias=AliasChoices("RUN_LOG_TAIL_MAX_BYTES"),
        description=(
            "Max bytes read from the tail of a VM's run.log via a GCS byte-range read "
            "(never the full object — observed sizes run 362KB-13.4MB). Bounds both API "
            "memory and response size regardless of the real object size."
        ),
    )

    run_log_tail_max_lines: int = Field(
        default=300,
        validation_alias=AliasChoices("RUN_LOG_TAIL_MAX_LINES"),
        description="Max lines returned by the run.log tail endpoint, taken from the end of the byte-range read.",
    )

    run_log_download_url_expiry_minutes: int = Field(
        default=15,
        validation_alias=AliasChoices("RUN_LOG_DOWNLOAD_URL_EXPIRY_MINUTES"),
        description=(
            "Validity window for the signed download URL returned by GET .../run-log/download (decision 4: "
            "short-lived, client downloads directly from GCS — the API never streams the object itself)."
        ),
    )

    # =========================================================================
    # DERIVED PROPERTIES
    # =========================================================================

    @property
    def effective_execution_store_bucket(self) -> str:
        """Get the effective execution store bucket with project_id fallback.

        execution-store is a FLAT env-tiered yaml kind post Fold C
        (bucket_fold_execution_strategy_2026_07_17.md): the resolver folds the old
        per-asset_group CEFI/TRADFI/DEFI/SPORTS dict into a single
        ``execution-store-{env}-{pid}`` bucket and IGNORES asset_group (asset_group
        is now an object-key prefix ``{ag}/`` within that shared bucket). This
        property backs a single global /config-buckets route entry
        (routes/services.py get_config_buckets, "execution-service"); the
        ``asset_group="cefi"`` passed below is vestigial — ignored for the flat kind
        (same mechanism strategy-store uses) — and kept only so resolution stays
        robust across the fold transition. The route advertises the config path
        under the ``cefi/`` prefix since configs now live at ``{ag}/configs/``.
        """
        if self.execution_store_bucket:
            return self.execution_store_bucket
        return resolve_bucket_name(cloud=cast(Cloud, self.cloud_provider), kind="execution-store", asset_group="cefi")

    @property
    def effective_strategy_store_cefi_bucket(self) -> str:
        """Get the effective strategy store CeFi bucket with project_id fallback.

        strategy-store is a FLAT yaml kind (unified bucket per D6 Phase 4 operator
        decision 2026-05-20) — the resolver ignores asset_group for flat kinds, so
        this intentionally returns the SAME bucket as the tradfi/defi variants
        below. That's correct: one strategy-store bucket for all asset_groups.
        """
        if self.strategy_store_cefi_bucket:
            return self.strategy_store_cefi_bucket
        return resolve_bucket_name(cloud=cast(Cloud, self.cloud_provider), kind="strategy-store")

    @property
    def effective_strategy_store_tradfi_bucket(self) -> str:
        """Get the effective strategy store TradFi bucket with project_id fallback.

        strategy-store is FLAT (see effective_strategy_store_cefi_bucket) — same
        bucket as cefi/defi by design.
        """
        if self.strategy_store_tradfi_bucket:
            return self.strategy_store_tradfi_bucket
        return resolve_bucket_name(cloud=cast(Cloud, self.cloud_provider), kind="strategy-store")

    @property
    def effective_strategy_store_defi_bucket(self) -> str:
        """Get the effective strategy store DeFi bucket with project_id fallback.

        strategy-store is FLAT (see effective_strategy_store_cefi_bucket) — same
        bucket as cefi/tradfi by design.
        """
        if self.strategy_store_defi_bucket:
            return self.strategy_store_defi_bucket
        return resolve_bucket_name(cloud=cast(Cloud, self.cloud_provider), kind="strategy-store")

    @property
    def effective_ml_configs_store_bucket(self) -> str:
        """Get the effective ML configs store bucket with project_id fallback.

        ml FOLD B (bucket_fold_ml_2026_07_17.md): ``resolve_bucket_name(kind=
        'ml-store')`` returns the SINGLE folded ``ml-store-{env}-{pid}`` bucket. The ML
        configs live under the ``configs/`` object-key PREFIX within that shared bucket
        (callers that build a display path prepend ``configs/`` — see routes/services.py).
        The configs/ fold was empty at fold time (2026-07-13 estate audit), so this cutover
        is data-safe. (The ``ml-configs-store`` alias was retired 2026-07-19; resolve the
        folded ``ml-store`` kind directly.)
        """
        if self.ml_configs_store_bucket:
            return self.ml_configs_store_bucket
        return resolve_bucket_name(cloud=cast(Cloud, self.cloud_provider), kind="ml-store")

    @property
    def effective_ml_training_artifacts_bucket(self) -> str:
        """Get the effective ML training-artifacts bucket.

        ml FOLD B (bucket_fold_ml_2026_07_17.md): ``resolve_bucket_name(kind='ml-store')``
        returns the SINGLE folded ``ml-store-{env}-{pid}`` bucket. Training-metrics /
        experiment-output artefacts live under the ``training-artifacts/`` object-key PREFIX
        within that shared bucket (callers that build a blob path prepend
        ``training-artifacts/`` — see commentary/pipeline_uat.py). (The ``ml-training-artifacts``
        alias was retired 2026-07-19; resolve the folded ``ml-store`` kind directly.)
        """
        return resolve_bucket_name(cloud=cast(Cloud, self.cloud_provider), kind="ml-store")

    @property
    def effective_state_bucket(self) -> str:
        """Get the effective state bucket with fallback."""
        if self.state_bucket:
            return self.state_bucket
        if self.cloud_provider == "aws":
            return f"unified-deployment-state-{self.aws_account_id}"
        return f"unified-deployment-state-{self.gcp_project_id}"

    @property
    def all_failover_regions(self) -> list[str]:
        """Get failover regions based on cloud provider."""
        if self.cloud_provider == "aws":
            return ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]
        return ["asia-northeast1", "us-central1", "us-east1", "europe-west1"]

    @property
    def effective_github_token_sa(self) -> str:
        """Get the effective GitHub token service account with fallback."""
        if self.github_token_sa:
            return self.github_token_sa
        return f"github-token-sa@{self.gcp_project_id}.iam.gserviceaccount.com"

    @property
    def effective_port(self) -> str:
        """Get effective port with Cloud Run convention fallback."""
        if self.port:
            return self.port
        if self.api_port != "8000":
            return self.api_port
        return "8080"

    @property
    def effective_project_id(self) -> str:
        """Get the effective GCP project ID."""
        return self.gcp_project_id or ""

    @property
    def effective_region(self) -> str:
        """Get the effective GCS region.

        Defaults to ``asia-northeast1`` — the workspace SSOT region for ALL GCS
        data, Cloud Build triggers, and the Artifact Registry (CLAUDE.md: "all
        GCS data is in asia-northeast1; zone default asia-northeast1-c"). The
        prior ``us-central1`` default mislocated VM-launch zones / GCS region /
        cross-region-egress checks for this workspace (B1-followup anomaly,
        operator decision 2026-06-12: fix default → asia-northeast1).
        """
        return self.gcs_region or "asia-northeast1"

    def require_gcp_project_id(self) -> str:
        """GCP project id, or fail loud when unconfigured.

        Replaces the prior hardcoded prod-project fallback (QG
        hardcoded-project-id ratchet; codex_violations_ratchet_to_five
        plan 2026-06-10): production always sets ``GCP_PROJECT_ID``
        (Cloud Run env), tests set ``test-project``
        (``tests/unit/conftest.py``), so an empty value is a deployment
        misconfiguration — never silently target the prod project.
        """
        if not self.gcp_project_id:
            raise RuntimeError(
                "GCP_PROJECT_ID is not configured — set the env var; hardcoded prod-project fallbacks are banned"
            )
        return self.gcp_project_id
