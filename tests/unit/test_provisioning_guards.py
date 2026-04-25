"""Unit tests for provisioning guard helpers."""

from __future__ import annotations

import pytest
from fastapi import HTTPException
from unified_api_contracts.internal.schemas.rbac import UserRole

from deployment_api.security.provisioning_guards import (
    ensure_role_mutation_allowed,
    parse_org_owner_allowlist,
)


def test_parse_org_owner_allowlist() -> None:
    raw = " Owner@Example.com , femi@odum-research.co.uk "
    got = parse_org_owner_allowlist(raw)
    assert got == {"owner@example.com", "femi@odum-research.co.uk"}


def test_disable_auth_skips_checks() -> None:
    ensure_role_mutation_allowed(
        disable_auth=True,
        actor_email=None,
        actor_role=UserRole.VIEWER,
        target_email="a@b.com",
        old_role=UserRole.VIEWER,
        new_role=UserRole.SUPER_ADMIN,
        org_owner_emails=set(),
    )


def test_requires_actor_email_when_auth_enabled() -> None:
    with pytest.raises(HTTPException) as exc:
        ensure_role_mutation_allowed(
            disable_auth=False,
            actor_email=None,
            actor_role=UserRole.SUPER_ADMIN,
            target_email="a@b.com",
            old_role=UserRole.VIEWER,
            new_role=UserRole.OPERATOR,
            org_owner_emails=set(),
        )
    assert exc.value.status_code == 400


def test_privileged_roles_require_org_owner() -> None:
    owners = parse_org_owner_allowlist("owner@example.com")
    with pytest.raises(HTTPException) as exc:
        ensure_role_mutation_allowed(
            disable_auth=False,
            actor_email="admin@corp.com",
            actor_role=UserRole.SUPER_ADMIN,
            target_email="victim@corp.com",
            old_role=UserRole.VIEWER,
            new_role=UserRole.ADMIN,
            org_owner_emails=owners,
        )
    assert exc.value.status_code == 403

    ensure_role_mutation_allowed(
        disable_auth=False,
        actor_email="owner@example.com",
        actor_role=UserRole.ADMIN,
        target_email="victim@corp.com",
        old_role=UserRole.VIEWER,
        new_role=UserRole.SUPER_ADMIN,
        org_owner_emails=owners,
    )


def test_self_elevation_blocked() -> None:
    with pytest.raises(HTTPException) as exc:
        ensure_role_mutation_allowed(
            disable_auth=False,
            actor_email="user@corp.com",
            actor_role=UserRole.ADMIN,
            target_email="user@corp.com",
            old_role=UserRole.VIEWER,
            new_role=UserRole.ADMIN,
            org_owner_emails=parse_org_owner_allowlist("user@corp.com"),
        )
    assert exc.value.status_code == 403


def test_cannot_grant_above_actor_unless_owner() -> None:
    with pytest.raises(HTTPException) as exc:
        ensure_role_mutation_allowed(
            disable_auth=False,
            actor_email="admin@corp.com",
            actor_role=UserRole.ADMIN,
            target_email="peer@corp.com",
            old_role=UserRole.VIEWER,
            new_role=UserRole.SUPER_ADMIN,
            org_owner_emails=set(),
        )
    assert exc.value.status_code == 403
