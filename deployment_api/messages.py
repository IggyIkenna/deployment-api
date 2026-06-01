"""HTTP error message constants — i18n readiness catalog (S15 audit 2026-05-18).

All user-visible error detail strings in deployment_api routes must be
sourced from this module so they are discoverable, auditable, and easy
to swap for translated copies in a future i18n pass.
"""

from __future__ import annotations

# 500 / generic server errors
INTERNAL_ERROR: str = "Internal error — see server logs"
INTERNAL_SERVER_ERROR: str = "Internal server error"

# 503 / dependency unavailability
CLOUD_SERVICE_UNAVAILABLE: str = "Cloud service temporarily unavailable"
SERVICE_TEMPORARILY_UNAVAILABLE: str = "Service temporarily unavailable"

# 400 / configuration / validation
SERVICE_CONFIG_NOT_FOUND: str = "Service configuration not found"
INVALID_CONFIG: str = "Invalid configuration — see logs"
INVALID_PARAMETERS: str = "Invalid parameters — see server logs"

# 504 / timeout
LAUNCHER_TIMED_OUT: str = "Launcher timed out"

# 502 / upstream failures
EXECUTION_SERVICE_UNREACHABLE_UNHOLD: str = "execution-service unreachable — cannot unhold instruction"
EXECUTION_SERVICE_UNREACHABLE_REJECT: str = "execution-service unreachable — cannot reject instruction"
