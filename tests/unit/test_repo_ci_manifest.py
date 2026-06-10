"""Unit tests for the workspace-manifest accessor (ManifestView) — fixture-driven."""

from __future__ import annotations

from deployment_api.routes._repo_ci_manifest import manifest_view_from_raw

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

    def test_empty_manifest_safe(self) -> None:
        view = manifest_view_from_raw({})
        assert view.repos == []
        assert view.breaking_pending == []
        assert not view.staging_locked
        assert view.ci_status_for("anything") == "UNKNOWN"
