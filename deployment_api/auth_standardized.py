"""API authentication — delegates to shared UCI middleware."""

from unified_trading_library.cloud_interface import AuthContext, create_api_auth

# Create the auth dependency for this API
api_auth = create_api_auth("deployment-api")

# Re-export for route usage
__all__ = ["AuthContext", "api_auth"]
