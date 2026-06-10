"""Unit tests for the image/build signal in routes/repo_ci.py.

Pins the cloud-toggle parity (operator add 2026-06-10): the build half of the
aggregator dispatches on the active provider — GCP Cloud Build triggers vs AWS
CodeBuild projects — and degrades to honest-unknown ({}) on any cloud failure or
inactive/unavailable provider. The GitHub/manifest half is cloud-agnostic.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from deployment_api.routes import repo_ci

_MOD = "deployment_api.routes.repo_ci"


@pytest.fixture(autouse=True)
def _reset_builds_cache() -> None:
    """The fetcher memoises in a module global — clear it so each test fetches fresh."""
    repo_ci._builds_cache = None  # pyright: ignore[reportPrivateUsage]


class TestGcpBuildSignal:
    async def test_gcp_maps_repo_to_status_and_sha(self) -> None:
        triggers = [
            {"trigger_id": "t-mtds", "service": "market-tick-data-service"},
            {"trigger_id": "t-exec", "service": "execution-service"},
        ]
        builds = {
            "t-mtds": {"status": "SUCCESS", "commit_sha": "abc1234"},
            "t-exec": {"status": "FAILURE", "commit_sha": "def5678"},
        }

        async def _fake_recent(_ids: list[str]) -> dict[str, dict[str, str]]:
            return builds

        with (
            patch(f"{_MOD}.is_aws_provider", return_value=False),
            patch(f"{_MOD}._build_trigger_list_sync", return_value=triggers),
            patch(f"{_MOD}._get_recent_builds_for_triggers", side_effect=_fake_recent),
        ):
            result = await repo_ci._latest_builds_by_repo()  # pyright: ignore[reportPrivateUsage]

        assert result == {
            "market-tick-data-service": ("SUCCESS", "abc1234"),
            "execution-service": ("FAILURE", "def5678"),
        }


class TestAwsBuildSignal:
    async def test_aws_maps_project_to_status_and_sha(self) -> None:
        # CodeBuild projects are the AWS equivalent of triggers (trigger_id == project name).
        projects = [
            {"trigger_id": "deployment-api-build", "service": "deployment-api"},
            {"trigger_id": "greeks-service-build", "service": "greeks-service"},
        ]
        builds = {
            "deployment-api-build": {"status": "SUCCESS", "commit_sha": "aaa1111"},
            "greeks-service-build": {"status": "WORKING", "commit_sha": "bbb2222"},
        }

        with (
            patch(f"{_MOD}.is_aws_provider", return_value=True),
            patch(f"{_MOD}.list_codebuild_projects_sync", return_value=projects),
            patch(f"{_MOD}.get_recent_builds_for_projects_sync", return_value=builds),
        ):
            result = await repo_ci._latest_builds_by_repo()  # pyright: ignore[reportPrivateUsage]

        assert result == {
            "deployment-api": ("SUCCESS", "aaa1111"),
            "greeks-service": ("WORKING", "bbb2222"),
        }

    async def test_aws_none_build_stays_honestly_unknown(self) -> None:
        # A project with no builds yields None — that repo must be ABSENT (image_stale=None),
        # never a fabricated row.
        projects = [
            {"trigger_id": "deployment-api-build", "service": "deployment-api"},
            {"trigger_id": "ml-service-build", "service": "ml-service"},
        ]
        builds: dict[str, dict[str, str] | None] = {
            "deployment-api-build": {"status": "SUCCESS", "commit_sha": "aaa1111"},
            "ml-service-build": None,
        }

        with (
            patch(f"{_MOD}.is_aws_provider", return_value=True),
            patch(f"{_MOD}.list_codebuild_projects_sync", return_value=projects),
            patch(f"{_MOD}.get_recent_builds_for_projects_sync", return_value=builds),
        ):
            result = await repo_ci._latest_builds_by_repo()  # pyright: ignore[reportPrivateUsage]

        assert result == {"deployment-api": ("SUCCESS", "aaa1111")}
        assert "ml-service" not in result


class TestBuildSignalDegradesHonestly:
    async def test_cloud_failure_yields_empty_map(self) -> None:
        # boto3 / google.api_core exceptions (outside OSError/ValueError) must degrade to {},
        # never kill the overview (live 500 root cause, 2026-06-10).
        def _boom() -> list[dict[str, str]]:
            raise RuntimeError("InvalidArgument: region/project mismatch")

        with (
            patch(f"{_MOD}.is_aws_provider", return_value=False),
            patch(f"{_MOD}._build_trigger_list_sync", side_effect=_boom),
        ):
            result = await repo_ci._latest_builds_by_repo()  # pyright: ignore[reportPrivateUsage]

        assert result == {}

    async def test_image_signal_reads_none_when_no_build_data(self) -> None:
        # The downstream image signal: no build data for a repo → image_stale is honestly unknown.
        from deployment_api.routes._repo_ci_manifest import ManifestView

        view = ManifestView({})  # empty manifest → deployed_version_for is None
        signal = repo_ci._image_signal(view, "execution-service", main_sha="abc1234", builds={})  # pyright: ignore[reportPrivateUsage]
        assert signal["image_stale"] is None
        assert signal["last_build_status"] is None
        assert signal["last_build_sha"] is None
