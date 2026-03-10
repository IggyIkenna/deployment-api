"""Service modules for deployment business logic."""


# Placeholder classes defined first to break the circular import chain:
# services/__init__ → deployment_manager → routes/deployment_validation
# → routes/__init__ → data_status.py → services (needs these classes)
class DataStatusService:
    """Placeholder for DataStatusService."""

    pass


class DataQueryService:
    """Placeholder for DataQueryService."""

    pass


class DataAnalyticsService:
    """Placeholder for DataAnalyticsService."""

    pass


from .deployment_manager import DeploymentManager  # noqa: E402
from .deployment_state import DeploymentStateManager  # noqa: E402
from .sync_service import SyncService  # noqa: E402

__all__ = [
    "DataAnalyticsService",
    "DataQueryService",
    "DataStatusService",
    "DeploymentManager",
    "DeploymentStateManager",
    "SyncService",
]
