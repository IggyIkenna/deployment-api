"""Application lifespan and event logging."""

from contextlib import asynccontextmanager

from unified_events_interface import log_event, setup_events
from unified_events_interface import MockEventSink


@asynccontextmanager
async def lifespan(app: object):
    """Lifespan context: setup events on startup, log STOPPED on shutdown."""
    setup_events(
        service_name="deployment-api",
        mode="live",
        sink=MockEventSink(),
    )
    log_event("STARTED")
    yield
    log_event("STOPPED")
