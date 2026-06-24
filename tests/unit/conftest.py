"""
Unit test configuration - mocks broken UTL imports before any module imports.

unified_trading_library has a broken CloudTarget/StandardizedDomainCloudService
import chain that prevents importing deployment_api.settings. We mock these
at the sys.modules level before any test module is collected.

We also pre-mock deployment_api.services as a proper package-like mock so that
both flat imports (from deployment_api.services import SyncService) and
sub-module imports (deployment_api.services.deployment_manager) work correctly,
regardless of the order tests are collected.
"""

import sys
from types import ModuleType
from unittest.mock import MagicMock


def _ensure_utl_mocked() -> None:
    """No-op: unified_trading_library is now properly installed and importable.

    Previously this function pre-mocked UTL to work around broken import chains
    in deployment_api.settings. The settings module now uses DeploymentApiConfig
    (local to deployment-api), so the UTL mock is no longer needed.
    """


def _ensure_services_mocked() -> None:
    """Pre-mock deployment_api.services as a package-compatible module.

    Both flat imports (from deployment_api.services import SyncService) and
    dotted sub-module imports (from deployment_api.services.deployment_manager
    import DeploymentManager) must work. We achieve this by creating real
    ModuleType objects with MagicMock attributes and registering all sub-modules
    in sys.modules before any test file is collected.

    user_management is loaded as a REAL module (not mocked) because it defines
    Pydantic request/response models used as FastAPI type annotations in routes.
    MagicMock cannot substitute for Pydantic BaseModel subclasses.
    """
    if "deployment_api.services" in sys.modules:
        return

    # Load user_management as a REAL module BEFORE replacing the services package.
    # It defines Pydantic BaseModel subclasses (CreateUserRequest, AssignRoleRequest,
    # UpdateUserRequest) used as FastAPI route parameter types — MagicMock cannot
    # substitute for these.
    import importlib

    real_um = importlib.import_module("deployment_api.services.user_management")

    # Load data_status_drilldown as a REAL module BEFORE the services package is
    # replaced below. Its tests patch functions inside it directly, and the
    # module has no circular-import risk (only depends on UAC + storage facade).
    real_drilldown = importlib.import_module("deployment_api.services.data_status_drilldown")

    # data_status_mock is a pure-function seed module with no circular import
    # risk. Load it as a real module so routes/data_status.py can import from
    # it when mock_mode is enabled under test.
    real_mock = importlib.import_module("deployment_api.services.data_status_mock")

    # shard_detail is a read-only drill-down service that depends on
    # data_status_drilldown (already loaded above) + UAC + storage_facade —
    # no circular risk; tests monkey-patch module-level attrs directly.
    real_shard_detail = importlib.import_module("deployment_api.services.shard_detail")

    # coverage_drift is a pure pandas/UAC consumer (Phase 8.B). Load BEFORE
    # the services-package stub replacement so its real implementation is
    # available to tests.
    real_coverage_drift = importlib.import_module("deployment_api.services.coverage_drift")

    # data_status_hierarchical is a pure UAC + UTL + drilldown-facade
    # consumer (drilldown plan Phase 1). Load BEFORE the stub replacement
    # so the new /api/data-status/drilldown route's tests can patch the
    # ``read_availability_index`` attribute on the real module.
    real_hierarchical = importlib.import_module("deployment_api.services.data_status_hierarchical")

    # deploy_missing is a pure-function module (drilldown plan Phase 3).
    # Loaded for the same reason as data_status_hierarchical above.
    real_deploy_missing = importlib.import_module("deployment_api.services.deploy_missing")

    # tarball_staleness is a standalone helper (deploy-missing auto-launch
    # plan Phase 1). Loaded as a real module so its unit tests can import
    # the public surface (TarballStalenessChecker, RefreshResult, etc.)
    # rather than colliding with the MagicMock services package below.
    real_tarball_staleness = importlib.import_module("deployment_api.services.tarball_staleness")

    # deploy_missing_launch is the Phase 2 auto-launch service
    # (deploy_missing_auto_launch_2026_05_07.md Phase 2). Loaded as a real
    # module so its unit tests can import DeployMissingRateLimiter etc.
    # without hitting the fake services package's empty __path__.
    real_deploy_missing_launch = importlib.import_module("deployment_api.services.deploy_missing_launch")

    # Build the top-level services package module (replacing the real one)
    services_mod = ModuleType("deployment_api.services")
    services_mod.__package__ = "deployment_api.services"
    services_mod.__path__ = []  # type: ignore[attr-defined]  # marks it as a package

    # Expose common service classes at the package level
    services_mod.SyncService = MagicMock()  # type: ignore[attr-defined]
    services_mod.DataAnalyticsService = MagicMock()  # type: ignore[attr-defined]
    services_mod.DataQueryService = MagicMock()  # type: ignore[attr-defined]
    services_mod.DataStatusService = MagicMock()  # type: ignore[attr-defined]

    sys.modules["deployment_api.services"] = services_mod

    # Re-register user_management as a real module on the fake services package
    sys.modules["deployment_api.services.user_management"] = real_um
    services_mod.user_management = real_um

    # Re-register data_status_drilldown as a real module on the fake package
    sys.modules["deployment_api.services.data_status_drilldown"] = real_drilldown
    services_mod.data_status_drilldown = real_drilldown

    # Re-register data_status_mock as a real module on the fake package
    sys.modules["deployment_api.services.data_status_mock"] = real_mock
    services_mod.data_status_mock = real_mock

    # Re-register shard_detail as a real module on the fake package
    sys.modules["deployment_api.services.shard_detail"] = real_shard_detail
    services_mod.shard_detail = real_shard_detail

    # Re-register coverage_drift (already imported above before the stub
    # replacement) as a real module on the fake services package (Phase 8.B).
    sys.modules["deployment_api.services.coverage_drift"] = real_coverage_drift
    services_mod.coverage_drift = real_coverage_drift

    # Re-register data_status_hierarchical as a real module (drilldown
    # plan Phase 1).
    sys.modules["deployment_api.services.data_status_hierarchical"] = real_hierarchical
    services_mod.data_status_hierarchical = real_hierarchical

    # Re-register deploy_missing as a real module (drilldown plan Phase 3).
    sys.modules["deployment_api.services.deploy_missing"] = real_deploy_missing
    services_mod.deploy_missing = real_deploy_missing

    # Re-register tarball_staleness as a real module (deploy-missing
    # auto-launch plan Phase 1).
    sys.modules["deployment_api.services.tarball_staleness"] = real_tarball_staleness
    services_mod.tarball_staleness = real_tarball_staleness

    # Re-register deploy_missing_launch as a real module (Phase 2 auto-launch).
    sys.modules["deployment_api.services.deploy_missing_launch"] = real_deploy_missing_launch
    services_mod.deploy_missing_launch = real_deploy_missing_launch

    # Sub-module list — only modules that need mocking (circular import breakers).
    # Real modules (sync_service, event_processor, state_manager) are NOT mocked
    # because their tests import and test them directly.
    sub_modules = [
        "data_analytics_service",
        "data_query_service",
        "data_status_service",
        "deployment_manager",
        "deployment_state",
    ]

    for sub in sub_modules:
        full_name = f"deployment_api.services.{sub}"
        if full_name not in sys.modules:
            sub_mod = ModuleType(full_name)
            sub_mod.__package__ = "deployment_api.services"
            # Expose plausible class names on each sub-module via MagicMock
            sub_mod.DeploymentManager = MagicMock()  # type: ignore[attr-defined]
            sub_mod.DeploymentStateManager = MagicMock()  # type: ignore[attr-defined]
            sub_mod.SyncService = MagicMock()  # type: ignore[attr-defined]
            sub_mod.DataAnalyticsService = MagicMock()  # type: ignore[attr-defined]
            sub_mod.DataQueryService = MagicMock()  # type: ignore[attr-defined]
            sub_mod.DataStatusService = MagicMock()  # type: ignore[attr-defined]
            sub_mod.StateManager = MagicMock()  # type: ignore[attr-defined]
            sub_mod.EventProcessor = MagicMock()  # type: ignore[attr-defined]
            sys.modules[full_name] = sub_mod
            # Also set as attribute on the package module
            setattr(services_mod, sub, sub_mod)


def _ensure_external_packages_mocked() -> None:
    """Pre-mock external packages (backends, deployment) as package-compatible modules.

    These packages are not installed in the test environment but are imported at
    module level by some source files. We register proper ModuleType objects (not
    flat MagicMocks) so that dotted sub-module imports work regardless of
    the order in which test files are collected.
    """
    # --- Shared status sentinel objects (created unconditionally) ---
    # These are referenced by both the deployment.state and deployment_service.deployment.state
    # mocks so tests and source code compare the same objects.
    _shared_deployment_status = MagicMock()
    _shared_shard_status = MagicMock()
    _shared_state_manager_cls = MagicMock()

    # --- backends package ---
    if "backends" not in sys.modules:
        backends_mod = ModuleType("backends")
        backends_mod.__package__ = "backends"
        backends_mod.__path__ = []  # type: ignore[attr-defined]
        sys.modules["backends"] = backends_mod

        for sub_name, attrs in {
            "base": {"JobStatus": MagicMock(SUCCEEDED="SUCCEEDED", FAILED="FAILED", RUNNING="RUNNING")},
            "cloud_run": {"CloudRunBackend": MagicMock()},
            "vm": {"VMBackend": MagicMock()},
        }.items():
            full = f"backends.{sub_name}"
            sub_mod = ModuleType(full)
            sub_mod.__package__ = "backends"
            for attr, val in attrs.items():
                setattr(sub_mod, attr, val)
            sys.modules[full] = sub_mod
            setattr(backends_mod, sub_name, sub_mod)

    # --- deployment package ---
    if "deployment" not in sys.modules:
        dep_pkg = ModuleType("deployment")
        dep_pkg.__package__ = "deployment"
        dep_pkg.__path__ = []  # type: ignore[attr-defined]
        dep_pkg.StateManager = _shared_state_manager_cls  # type: ignore[attr-defined]
        sys.modules["deployment"] = dep_pkg

        for sub_name, attrs in {
            "state": {
                "DeploymentStatus": _shared_deployment_status,
                "StateManager": _shared_state_manager_cls,
                "ShardStatus": _shared_shard_status,
            },
            "orchestrator": {"DeploymentOrchestrator": MagicMock()},
            "quota_broker_client": {"QuotaBrokerClient": MagicMock()},
        }.items():
            full = f"deployment.{sub_name}"
            sub_mod = ModuleType(full)
            sub_mod.__package__ = "deployment"
            for attr, val in attrs.items():
                setattr(sub_mod, attr, val)
            sys.modules[full] = sub_mod
            setattr(dep_pkg, sub_name, sub_mod)

    # --- deployment_service package ---
    if "deployment_service" not in sys.modules:
        ds_pkg = ModuleType("deployment_service")
        ds_pkg.__package__ = "deployment_service"
        ds_pkg.__path__ = []  # type: ignore[attr-defined]
        sys.modules["deployment_service"] = ds_pkg

        _mock_validator = MagicMock(get_required=MagicMock(return_value="value"))

        # Build a realistic DeploymentConfig mock so that settings.py constants
        # resolve to proper types (ints, strs, lists) rather than MagicMocks.
        _mock_config_instance = MagicMock()
        # Core cloud
        _mock_config_instance.gcp_project_id = "test-project"
        _mock_config_instance.gcs_region = "us-central1"
        _mock_config_instance.effective_state_bucket = "test-bucket"
        _mock_config_instance.service_account_email = "sa@test.iam"
        _mock_config_instance.github_org = "test-org"
        _mock_config_instance.effective_github_token_sa = "test-token"
        _mock_config_instance.cloud_provider = "gcp"
        _mock_config_instance.deployment_env = "development"
        # Server
        _mock_config_instance.api_port = 8080
        _mock_config_instance.workers = 1
        _mock_config_instance.effective_port = 8080
        _mock_config_instance.frontend_port = 3000
        _mock_config_instance.cors_allowed_origins = "http://localhost:3000"
        _mock_config_instance.cors_allowed_cloud_run = False
        # Auto-sync
        _mock_config_instance.auto_sync_enabled = True
        _mock_config_instance.auto_sync_interval_seconds = 30
        _mock_config_instance.auto_sync_interval_active = 10
        _mock_config_instance.auto_sync_lock_ttl_seconds = 120
        _mock_config_instance.auto_sync_max_parallel = 4
        # Orphan cleanup
        _mock_config_instance.orphan_delete_max_parallel = 10
        _mock_config_instance.orphan_delete_retry_seconds = 120
        _mock_config_instance.orphan_cleanup_recently_completed_minutes = 30
        # Quota / write
        _mock_config_instance.write_quota_buffer = 0.1
        # Concurrency
        _mock_config_instance.default_max_concurrent = 10
        _mock_config_instance.max_concurrent_hard_limit = 50
        # Auto-scheduler
        _mock_config_instance.auto_scheduler_max_launch_per_tick = 5
        _mock_config_instance.auto_scheduler_max_releases_per_tick = 5
        _mock_config_instance.auto_scheduler_batch_size = 10
        _mock_config_instance.auto_scheduler_inter_batch_delay = 0.1
        _mock_config_instance.auto_scheduler_delete_batch_size = 5
        _mock_config_instance.auto_scheduler_delete_batch_delay_seconds = 0.5
        _mock_config_instance.auto_scheduler_parallel_workers = 2
        _mock_config_instance.auto_scheduler_vm_rate_limit = 2
        # Stuck / OOM
        _mock_config_instance.stuck_shard_grace_seconds = 300
        _mock_config_instance.oom_kill_threshold = 90
        # VM launch
        _mock_config_instance.vm_launch_mini_batch_size = 5
        _mock_config_instance.vm_launch_mini_batch_delay_seconds = 1.0
        _mock_config_instance.unknown_status_max_polls = 3
        # Pool sizes
        _mock_config_instance.gcs_pool_size = 4
        _mock_config_instance.compute_pool_size = 4
        _mock_config_instance.compute_pool_maxsize = 8
        # Cache / redis
        _mock_config_instance.redis_url = "redis://localhost:6379/0"
        _mock_config_instance.gcs_cache_path = "cache/"
        _mock_config_instance.data_status_cache_ttl_seconds = 300
        _mock_config_instance.exec_cache_ttl_seconds = 300
        # Quota broker
        _mock_config_instance.quota_broker_url = ""
        _mock_config_instance.quota_broker_auth_mode = "none"
        _mock_config_instance.quota_broker_timeout_seconds = 5
        _mock_config_instance.broker_max_wait_seconds = 30
        # Misc
        _mock_config_instance.workspace_root = "/tmp/test-workspace"
        _mock_config_instance.is_mock_mode.return_value = False
        _mock_config_instance.enforce_single_region = False
        _mock_config_instance.disable_auth = False
        _mock_config_instance.api_key = "test-api-key"
        _mock_config_instance.enable_cloud_run_origin = False
        _mock_config_instance.log_level = "INFO"
        _DeploymentConfig = MagicMock(return_value=_mock_config_instance)

        for sub_name, attrs in {
            "config": {},
            "config.config_validator": {
                "ConfigurationError": Exception,
                "ValidationUtils": _mock_validator,
            },
            "deployment_config": {"DeploymentConfig": _DeploymentConfig},
            "config_loader": {
                "ConfigLoader": MagicMock(),
                "substitute_env_vars": MagicMock(return_value={}),
            },
            "shard_calculator": {"ShardCalculator": MagicMock()},
            "cloud_client": {"CloudClient": MagicMock()},
            "deployment": {
                "StateManager": _shared_state_manager_cls,
                "DeploymentState": MagicMock(),
                "ShardState": MagicMock(),
                "DeploymentStatus": _shared_deployment_status,
                "ShardStatus": _shared_shard_status,
            },
            "deployment.state": {
                "DeploymentStatus": _shared_deployment_status,
                "StateManager": _shared_state_manager_cls,
                "ShardStatus": _shared_shard_status,
                "DeploymentState": MagicMock(),
                "ShardState": MagicMock(),
            },
            "deployment.orchestrator": {"DeploymentOrchestrator": MagicMock()},
            "deployment.quota_broker_client": {"QuotaBrokerClient": MagicMock()},
            "deployments_registry": {
                "ACTIVE_PREFIX": "deployments/active/",
                "ARCHIVE_PREFIX": "deployments/archive/",
                "DEFAULT_BUCKET": "test-deployments-bucket",
                "DeploymentRegistryEntry": MagicMock(),
                "DeploymentsRegistry": MagicMock(),
                "vm_run_log_rolling_uri": MagicMock(
                    return_value="gs://test-bucket/log-archive/rolling/20260101/vm/run.log"
                ),
                "vm_serial_rolling_uri": MagicMock(
                    return_value="gs://test-bucket/log-archive/serial-rolling/20260101/vm/serial-console.txt"
                ),
            },
        }.items():
            full = f"deployment_service.{sub_name}"
            sub_mod = ModuleType(full)
            sub_mod.__package__ = "deployment_service"
            # Mark sub-packages (like deployment) so they can have their own sub-modules
            if "." not in sub_name:
                sub_mod.__path__ = []  # type: ignore[attr-defined]
            for attr, val in attrs.items():
                setattr(sub_mod, attr, val)
            sys.modules[full] = sub_mod

        # The deployment-observability classification spine (cloud_run_job_registry +
        # deployment_classification) is PURE-python (UAC-only deps, no cloud SDK), so
        # load the REAL modules from the editable deployment-service source rather than
        # stub them — the inventory route's classification MUST exercise the real
        # resolver. With the parent `deployment_service` package stubbed (__path__=[]),
        # normal import won't reach them, so load by file spec.
        import importlib.util as _ilu
        from pathlib import Path as _Path

        _ds_src = _Path(__file__).resolve().parents[3] / "deployment-service" / "deployment_service"
        for _real_sub in ("deployment_classification", "cloud_run_job_registry", "deployment_cluster_registry"):
            _real_path = _ds_src / f"{_real_sub}.py"
            if _real_path.exists():
                _spec = _ilu.spec_from_file_location(f"deployment_service.{_real_sub}", _real_path)
                if _spec is not None and _spec.loader is not None:
                    _real_mod = _ilu.module_from_spec(_spec)
                    sys.modules[f"deployment_service.{_real_sub}"] = _real_mod
                    _spec.loader.exec_module(_real_mod)


# Set required env vars before any routes are imported.  deployment_api/routes/__init__.py
# eagerly imports ALL route modules, each creating a DeploymentApiConfig() singleton at
# module level.  Any test file that imports a single route (e.g. kill_switch_routes) triggers
# this chain.  These setdefaults ensure the configs are initialised with sane test defaults
# regardless of which test file happens to be collected first in a given xdist worker.
import os as _os

_os.environ.setdefault("CLOUD_MOCK_MODE", "true")
_os.environ.setdefault("CLOUD_PROVIDER", "local")
_os.environ.setdefault("GCP_PROJECT_ID", "test-project")
_os.environ.setdefault("DISABLE_AUTH", "true")
_os.environ.setdefault("MOCK_STATE_MODE", "deterministic")

# Run immediately at import time (before pytest collects tests)
_ensure_utl_mocked()
_ensure_services_mocked()
_ensure_external_packages_mocked()


from collections.abc import Generator as _Generator

import pytest as _pytest


@_pytest.fixture(autouse=True)
def _reset_rate_limit_windows() -> _Generator[None]:
    """Reset sliding-window rate-limiter state before/after every test.

    Two separate rate limiters share global/instance state across tests:
    1. endpoint_rate_limit() — stores timestamps in module-level _ENDPOINT_WINDOWS.
    2. RateLimitMiddleware — stores per-IP deques in self._windows on the main app
       instance.  TestClient always presents as "testclient" IP, so after 60 requests
       across any test files that share the main app the middleware returns 429.

    Clearing both before each test prevents cross-file 429 failures.
    """
    import deployment_api.rate_limiting as _rl

    _rl._ENDPOINT_WINDOWS.clear()

    if "deployment_api.main" in sys.modules:
        try:
            from deployment_api.middleware import RateLimitMiddleware as _RateLimitMiddleware

            _app = sys.modules["deployment_api.main"].app
            node = _app.middleware_stack
            depth = 0
            while node is not None and depth < 20:
                if isinstance(node, _RateLimitMiddleware):
                    node._windows.clear()  # type: ignore[attr-defined]
                    break
                node = getattr(node, "app", None)
                depth += 1
        except Exception:
            pass

    yield

    _rl._ENDPOINT_WINDOWS.clear()


@_pytest.fixture(autouse=True)
def _isolate_events_globals() -> _Generator[None]:
    """Snapshot + restore the unified_trading_library.events module globals around every test.

    test_lifespan exercises the real FastAPI lifespan, whose `fastapi_uei_lifespan` else-branch
    calls `setup_service_observability(mode=<non-test>, sink=<real sink>)` — flipping the shared
    `_mode`/`_writer` out of test mode and never restoring them. Later tests that call `log_event`
    (tarball `trigger_refresh`/`ensure_fresh`, vm_events real-mode) then raise
    "Event logging not initialized" / fail the real sink — a deterministic cross-file pollution
    surfaced under `pytest -n` work-stealing as 2-3 "flaky" failures. Restoring the globals here
    isolates the leak.
    """
    import unified_trading_library.events as _ev

    _saved = (_ev._mode, _ev._writer, _ev._service_name)  # pyright: ignore[reportPrivateUsage]
    try:
        yield
    finally:
        _ev._mode, _ev._writer, _ev._service_name = _saved  # pyright: ignore[reportPrivateUsage]
