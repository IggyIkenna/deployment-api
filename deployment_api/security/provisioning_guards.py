"""Server-side checks for user role mutations — see portal `docs/SECURITY_AUTH.md`.

Callers must identify the **acting human** with `X-Acting-User-Email` when `DISABLE_AUTH` is false.
Org owners (comma list in `ORG_OWNER_EMAILS`) may grant `admin` / `super_admin`.
"""

from __future__ import annotations

from fastapi import HTTPException
from unified_api_contracts.internal import UserRole

ROLE_RANK: dict[UserRole, int] = {
    UserRole.VIEWER: 0,
    UserRole.OPERATOR: 1,
    UserRole.ADMIN: 2,
    UserRole.SUPER_ADMIN: 3,
}

_PRIVILEGED_DESTINATION_ROLES: frozenset[UserRole] = frozenset({UserRole.ADMIN, UserRole.SUPER_ADMIN})


def normalize_email(value: str) -> str:
    return value.strip().lower()


def parse_org_owner_allowlist(csv: str) -> set[str]:
    if not csv.strip():
        return set()
    return {normalize_email(part) for part in csv.split(",") if part.strip()}


def ensure_role_mutation_allowed(
    *,
    disable_auth: bool,
    actor_email: str | None,
    actor_role: UserRole,
    target_email: str,
    old_role: UserRole,
    new_role: UserRole,
    org_owner_emails: set[str],
) -> None:
    """Raise HTTPException when a role change violates SECURITY_AUTH rules."""
    if disable_auth:
        return

    if actor_email is None or not str(actor_email).strip():
        raise HTTPException(
            status_code=400,
            detail="X-Acting-User-Email header is required for role mutations when auth is enabled",
        )

    actor = normalize_email(str(actor_email))
    target = normalize_email(target_email)

    if new_role in _PRIVILEGED_DESTINATION_ROLES and actor not in org_owner_emails:
        raise HTTPException(
            status_code=403,
            detail=("Granting admin or super_admin requires the actor email to be listed in ORG_OWNER_EMAILS"),
        )

    if actor == target and ROLE_RANK[new_role] > ROLE_RANK[old_role]:
        raise HTTPException(status_code=403, detail="Self-elevation is not permitted")

    if actor not in org_owner_emails and ROLE_RANK[new_role] > ROLE_RANK[actor_role]:
        raise HTTPException(
            status_code=403,
            detail=("Cannot grant a role above the actor's own role unless the actor is in ORG_OWNER_EMAILS"),
        )
