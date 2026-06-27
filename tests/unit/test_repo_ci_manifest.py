"""Unit tests for the workspace-manifest accessor (ManifestView) — fixture-driven."""

from __future__ import annotations

from deployment_api.routes._repo_ci_manifest import ManifestView, manifest_view_from_raw
from deployment_api.routes.repo_ci import _build_promotion_blocked

_FIXTURE: dict[str, object] = {
    "repositories": {
        "unified-trading-library": {
            "type": "library",
            "github_url": "https://github.com/IggyIkenna/unified-trading-library",
            "ci_status": "MAIN_GREEN",
        },
        "greeks-service": {
            "type": "service",
            "github_url": "https://github.com/IggyIkenna/greeks-service",
            "ci_status": "STAGING_GREEN",
        },
        "broken-meta": "not-a-dict",
    },
    "staging_status": {
        "locked": True,
        "locked_reason": "breaking cascade",
        "breaking_pending": ["greeks-service", "unified-trading-library"],
    },
    "deployed_versions": {"greeks-service": "0.9.1"},
}

_FIXTURE_CONSOLIDATES: dict[str, object] = {
    "repositories": {
        "features-service": {
            "type": "service",
            "consolidates": ["features-delta-one-service", "features-onchain-service"],
        },
        "ml-service": {"type": "service", "consolidates": ["ml-training-service", "ml-inference-service"]},
        "unified-trading-library": {"type": "library"},
    },
}


class TestManifestView:
    def test_repos_sorted_and_typed(self) -> None:
        view = manifest_view_from_raw(_FIXTURE)
        names = [r.name for r in view.repos]
        assert names == sorted(names)
        utl = next(r for r in view.repos if r.name == "unified-trading-library")
        assert utl.repo_type == "library"
        assert utl.github_url.endswith("/unified-trading-library")

    def test_non_dict_repo_meta_tolerated(self) -> None:
        view = manifest_view_from_raw(_FIXTURE)
        broken = next(r for r in view.repos if r.name == "broken-meta")
        assert broken.repo_type == "unknown"

    def test_ci_status_for(self) -> None:
        view = manifest_view_from_raw(_FIXTURE)
        assert view.ci_status_for("unified-trading-library") == "MAIN_GREEN"
        assert view.ci_status_for("greeks-service") == "STAGING_GREEN"
        assert view.ci_status_for("nonexistent") == "UNKNOWN"

    def test_breaking_pending(self) -> None:
        view = manifest_view_from_raw(_FIXTURE)
        assert view.breaking_pending == ["greeks-service", "unified-trading-library"]

    def test_staging_lock(self) -> None:
        view = manifest_view_from_raw(_FIXTURE)
        assert view.staging_locked
        assert view.staging_locked_reason == "breaking cascade"

    def test_deployed_version(self) -> None:
        view = manifest_view_from_raw(_FIXTURE)
        assert view.deployed_version_for("greeks-service") == "0.9.1"
        assert view.deployed_version_for("unified-trading-library") is None

    def test_release_version_for_manifest_seam(self) -> None:
        """No override (manifest_view_from_raw): release_version_for reads manifest versions{}."""
        view = manifest_view_from_raw({"versions": {"greeks-service": "0.9.0"}})
        assert view.release_version_for("greeks-service") == "0.9.0"
        assert view.release_version_for("absent") is None
        assert manifest_view_from_raw({}).release_version_for("any") is None

    def test_release_version_for_firestore_override_wins(self) -> None:
        """With versions_override (the resolve_release_version_map overlay), Firestore truth wins."""
        view = ManifestView(
            {"versions": {"greeks-service": "0.9.0"}},
            versions_override={"greeks-service": "0.9.2"},
        )
        # Overlay value, not the (stale) manifest cache.
        assert view.release_version_for("greeks-service") == "0.9.2"
        # Override is the authoritative map: a repo absent from it is None even if in manifest.
        assert view.release_version_for("not-in-override") is None

    def test_resolve_repo_exact_and_consolidated(self) -> None:
        # Canonical service→repo mapping = repositories[repo].consolidates[] (manifest-documented).
        view = manifest_view_from_raw(_FIXTURE_CONSOLIDATES)
        assert view.resolve_repo("features-service") == "features-service"
        assert view.resolve_repo("features-delta-one-service") == "features-service"
        assert view.resolve_repo("ml-inference-service") == "ml-service"
        assert view.resolve_repo("unified-trading-library") == "unified-trading-library"
        assert view.resolve_repo("not-a-thing") is None

    def test_empty_manifest_safe(self) -> None:
        view = manifest_view_from_raw({})
        assert view.repos == []
        assert view.breaking_pending == []
        assert not view.staging_locked
        assert view.ci_status_for("anything") == "UNKNOWN"
        assert view.promotion_failures() == {}
        assert view.promotion_quarantine() == {}


_PROMOTION_FIXTURE: dict[str, object] = {
    "repositories": {"execution-service": {"type": "service"}, "greeks-service": {"type": "service"}},
    "promotion_failures": {"execution-service": 1, "greeks-service": 2, "bad": "x", "bool-skip": True},
    "promotion_quarantine": {
        "greeks-service": {"since": "2026-06-11T08:00:00Z", "attempts": 2, "escalated": True},
        "bad-detail": "not-a-dict",
    },
}


class TestPromotionBlocked:
    def test_promotion_failures_typed_and_tolerant(self) -> None:
        view = manifest_view_from_raw(_PROMOTION_FIXTURE)
        f = view.promotion_failures()
        assert f == {"execution-service": 1, "greeks-service": 2}  # "x" dropped, True (bool) skipped

    def test_promotion_quarantine_tolerant(self) -> None:
        view = manifest_view_from_raw(_PROMOTION_FIXTURE)
        q = view.promotion_quarantine()
        assert q["greeks-service"]["escalated"] is True
        assert q["bad-detail"] == {}  # non-dict detail → empty

    def test_build_promotion_blocked_union_and_sort(self) -> None:
        blocked = _build_promotion_blocked(manifest_view_from_raw(_PROMOTION_FIXTURE))
        # Union of failures + quarantine; sort = quarantined first, then failures desc.
        # greeks-service (quarantined, 2 fails) → bad-detail (quarantined via key, 0 fails) →
        # execution-service (not quarantined, 1 fail).
        assert [b["repo"] for b in blocked] == ["greeks-service", "bad-detail", "execution-service"]
        gs = blocked[0]
        assert gs["quarantined"] is True and gs["failures"] == 2
        assert gs["since"] == "2026-06-11T08:00:00Z" and gs["attempts"] == 2 and gs["escalated"] is True
        bad = blocked[1]
        assert bad["quarantined"] is True and bad["failures"] == 0 and "since" not in bad
        ex = blocked[2]
        assert ex["quarantined"] is False and ex["failures"] == 1

    def test_build_promotion_blocked_empty(self) -> None:
        assert _build_promotion_blocked(manifest_view_from_raw({})) == []


_SEMVER_FIXTURE: dict[str, object] = {
    "versions": {"_note": "x", "a-svc": "0.4.0", "b-svc": "1.2.0", "c-svc": "0.9.0", "pm": "1.2.86"},
    "staging_versions": {
        "_note": "x",
        "a-svc": "0.5.0",  # ahead → pending
        "b-svc": "1.2.0",  # equal → not pending
        "c-svc": "0.10.0",  # ahead (10 > 9 numerically, not string-wise) → pending
        "pm": "1.2.45",  # BEHIND main (vestigial) → not pending
    },
}


class TestPendingVersionBumps:
    def test_pending_version_bumps_semver_aware(self) -> None:
        view = manifest_view_from_raw(_SEMVER_FIXTURE)
        # a-svc (0.4→0.5) + c-svc (0.9→0.10, numeric not lexical) are ahead; b-svc equal,
        # pm behind, _note skipped.
        assert view.pending_version_bumps() == ["a-svc", "c-svc"]

    def test_pending_version_bumps_empty_manifest(self) -> None:
        assert manifest_view_from_raw({}).pending_version_bumps() == []

    def test_pending_version_bumps_uses_firestore_versions_override(self) -> None:
        """The main version is read from the Firestore release overlay (not the manifest cache).

        Manifest versions{} says a-svc is at 0.4.0 (stale → staging 0.5.0 would look pending), but
        the LIVE registry says a-svc already released 0.5.0 — so it is NOT a pending bump. b-svc's
        live version is still behind staging, so it IS pending.
        """
        view = ManifestView(
            _SEMVER_FIXTURE,
            versions_override={"a-svc": "0.5.0", "b-svc": "1.1.0", "c-svc": "0.9.0", "pm": "1.2.86"},
        )
        # a-svc no longer pending (live==staging); b-svc now pending (1.1.0 < staging 1.2.0); c-svc pending.
        assert view.pending_version_bumps() == ["b-svc", "c-svc"]


_PROMOTION_MODEL_FIXTURE: dict[str, object] = {
    "repositories": {
        "deployment-api": {
            "type": "service",
            "promotion_model": "ldr_main",
        },
        "unified-trading-library": {
            "type": "library",
            # no promotion_model field — default staging→main path
        },
    },
}


class TestPromotionModel:
    def test_promotion_model_for_ldr_main(self) -> None:
        view = manifest_view_from_raw(_PROMOTION_MODEL_FIXTURE)
        assert view.promotion_model_for("deployment-api") == "ldr_main"

    def test_promotion_model_for_absent_returns_none(self) -> None:
        view = manifest_view_from_raw(_PROMOTION_MODEL_FIXTURE)
        assert view.promotion_model_for("unified-trading-library") is None

    def test_promotion_model_for_nonexistent_repo_returns_none(self) -> None:
        view = manifest_view_from_raw(_PROMOTION_MODEL_FIXTURE)
        assert view.promotion_model_for("does-not-exist") is None

    def test_promotion_model_for_empty_manifest_returns_none(self) -> None:
        assert manifest_view_from_raw({}).promotion_model_for("any-repo") is None
