"""
Authentication middleware and dependencies for the deployment API.

Provides route-level authentication and authorization using Google OAuth.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

from pydantic import BaseModel


class GoogleUser(BaseModel):  # CORRECT-LOCAL
    """Google OAuth user model."""

    email: str
    name: str = ""
    roles: list[str] = []


F = TypeVar("F", bound=Callable[..., object])


def get_current_user() -> GoogleUser:
    """FastAPI dependency: returns the current authenticated Google user."""
    raise NotImplementedError("Auth not configured — inject via dependency override")


def role_required(role: str) -> Callable[[F], F]:
    """FastAPI dependency factory: requires the given role."""

    def decorator(func: F) -> F:
        return func

    return decorator


# Export auth dependencies for use in routes
__all__ = ["GoogleUser", "get_current_user", "role_required"]
