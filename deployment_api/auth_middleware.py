"""
Authentication middleware and dependencies for the deployment API.

Provides route-level authentication and authorization using Google OAuth.
"""

from unified_trading_library import GoogleUser, get_current_user, role_required

# Export auth dependencies for use in routes
__all__ = ["GoogleUser", "get_current_user", "role_required"]
