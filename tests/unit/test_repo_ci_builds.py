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
from deployment_api.routes.repo_ci import BuildSignal

_MOD = "deployment_api.routes.repo_ci"


@pytest.fixture(autouse=True)
def _reset_builds_cache() -> None:
    """The fetcher memoises in a module global — clear it so each test fetches fresh."""
    repo_ci._builds_cache = None  # pyright: ignore[reportPrivateUsage]


class TestGcpBuildSignal:
    async def test_gcp_maps_repo_to_status_and_sha(self) -> None:
        # GCP half matches builds to repos by REPO_NAME substitution (robust to trigger
        # recreation + avoids the 400-rejecting build_trigger_id filter). The aggregator
        # consumes _recent_builds_by_repo_name() — per repo a (latest, last_success) pair.
        builds = {
            # latest == success (green repo).
            "market-tick-data-service": (
                {
                    "status": "SUCCESS",
                    "commit_sha": "abc1234",
                    "finish_time": "2026-06-11T07:30:00Z",
                    "log_url": "https://console.cloud.google.com/cloud-build/builds/m",
                },
                {
                    "status": "SUCCESS",
                    "commit_sha": "abc1234",
                    "finish_time": "2026-06-11T07:30:00Z",
                    "log_url": "https://console.cloud.google.com/cloud-build/builds/m",
                },
            ),
            # latest FAILED but an earlier SUCCESS exists → last good sha differs.
            "execution-service": (
                {"status": "FAILURE", "commit_sha": "def5678", "finish_time": None, "log_url": None},
                {
                    "status": "SUCCESS",
                    "commit_sha": "ccc9999",
                    "finish_time": "2026-06-10T01:00:00Z",
                    "log_url": "https://console.cloud.google.com/cloud-build/builds/s",
                },
            ),
        }

        async def _fake_by_repo(max_scan: int = 400) -> dict[str, tuple[dict[str, str | None], dict[str, str | None]]]:
            _ = max_scan
            return builds

        with (
            patch(f"{_MOD}.is_aws_provider", return_value=False),
            patch(f"{_MOD}._recent_builds_by_repo_name", side_effect=_fake_by_repo),
        ):
            result = await repo_ci._latest_builds_by_repo()  # pyright: ignore[reportPrivateUsage]

        # BuildSignal carries the latest build AND the last-successful build.
        assert result == {
            "market-tick-data-service": BuildSignal(
                status="SUCCESS",
                sha="abc1234",
                finish_time="2026-06-11T07:30:00Z",
                log_url="https://console.cloud.google.com/cloud-build/builds/m",
                success_sha="abc1234",
                success_time="2026-06-11T07:30:00Z",
                success_log_url="https://console.cloud.google.com/cloud-build/builds/m",
            ),
            "execution-service": BuildSignal(
                status="FAILURE",
                sha="def5678",
                finish_time=None,
                log_url=None,
                success_sha="ccc9999",
                success_time="2026-06-10T01:00:00Z",
                success_log_url="https://console.cloud.google.com/cloud-build/builds/s",
            ),
        }


class TestAwsBuildSignal:
    async def test_aws_maps_project_to_status_and_sha(self) -> None:
        # CodeBuild projects are the AWS equivalent of triggers (trigger_id == project name).
        projects = [
            {"trigger_id": "deployment-api-build", "service": "deployment-api"},
            {"trigger_id": "greeks-service-build", "service": "greeks-service"},
        ]
        builds = {
            "deployment-api-build": {
                "status": "SUCCESS",
                "commit_sha": "aaa1111",
                "finish_time": "2026-06-11T06:00:00Z",
                "log_url": "https://console.aws.amazon.com/codesuite/codebuild/builds/x",
            },
            "greeks-service-build": {
                "status": "WORKING",
                "commit_sha": "bbb2222",
                "finish_time": None,
                "log_url": None,
            },
        }

        with (
            patch(f"{_MOD}.is_aws_provider", return_value=True),
            patch(f"{_MOD}.list_codebuild_projects_sync", return_value=projects),
            patch(f"{_MOD}.get_recent_builds_for_projects_sync", return_value=builds),
        ):
            result = await repo_ci._latest_builds_by_repo()  # pyright: ignore[reportPrivateUsage]

        # AWS best-effort last-success: success_* = the latest build when it's green, else None.
        assert result == {
            "deployment-api": BuildSignal(
                status="SUCCESS",
                sha="aaa1111",
                finish_time="2026-06-11T06:00:00Z",
                log_url="https://console.aws.amazon.com/codesuite/codebuild/builds/x",
                success_sha="aaa1111",
                success_time="2026-06-11T06:00:00Z",
                success_log_url="https://console.aws.amazon.com/codesuite/codebuild/builds/x",
            ),
            "greeks-service": BuildSignal(status="WORKING", sha="bbb2222", finish_time=None, log_url=None),
        }

    async def test_aws_none_build_stays_honestly_unknown(self) -> None:
        # A project with no builds yields None — that repo must be ABSENT (image_stale=None),
        # never a fabricated row.
        projects = [
            {"trigger_id": "deployment-api-build", "service": "deployment-api"},
            {"trigger_id": "ml-service-build", "service": "ml-service"},
        ]
        builds: dict[str, dict[str, str | None] | None] = {
            "deployment-api-build": {
                "status": "SUCCESS",
                "commit_sha": "aaa1111",
                "finish_time": "2026-06-11T06:00:00Z",
                "log_url": None,
            },
            "ml-service-build": None,
        }

        with (
            patch(f"{_MOD}.is_aws_provider", return_value=True),
            patch(f"{_MOD}.list_codebuild_projects_sync", return_value=projects),
            patch(f"{_MOD}.get_recent_builds_for_projects_sync", return_value=builds),
        ):
            result = await repo_ci._latest_builds_by_repo()  # pyright: ignore[reportPrivateUsage]

        assert result == {
            "deployment-api": BuildSignal(
                status="SUCCESS",
                sha="aaa1111",
                finish_time="2026-06-11T06:00:00Z",
                log_url=None,
                success_sha="aaa1111",
                success_time="2026-06-11T06:00:00Z",
                success_log_url=None,
            )
        }
        assert "ml-service" not in result


class TestBuildSignalDegradesHonestly:
    async def test_cloud_failure_yields_empty_map(self) -> None:
        # boto3 / google.api_core exceptions (outside OSError/ValueError) must degrade to {},
        # never kill the overview (live 500 root cause, 2026-06-10).
        async def _boom(max_scan: int = 300) -> dict[str, dict[str, str | None]]:
            _ = max_scan
            raise RuntimeError("InvalidArgument: region/project mismatch")

        with (
            patch(f"{_MOD}.is_aws_provider", return_value=False),
            patch(f"{_MOD}._recent_builds_by_repo_name", side_effect=_boom),
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
        # B1: time + log_url are honestly-unknown too when there's no build data.
        assert signal["last_build_time"] is None
        assert signal["last_build_log_url"] is None
        # last-success fields honestly-unknown too.
        assert signal["last_success_sha"] is None
        assert signal["last_success_time"] is None
        assert signal["last_success_log_url"] is None

    def test_image_signal_surfaces_last_success_when_latest_failed(self) -> None:
        # Operator add: a red latest build must NOT hide the last good image. The signal carries
        # the last-success sha/time/log separately, and image_stale compares main vs the SUCCESS
        # sha (the sha actually baked into the running image), not the failed latest build's sha.
        from deployment_api.routes._repo_ci_manifest import ManifestView

        view = ManifestView({})
        builds = {
            "execution-service": BuildSignal(
                status="FAILURE",
                sha="def5678",
                finish_time="2026-06-11T09:00:00Z",
                log_url="https://console.cloud.google.com/cloud-build/builds/fail",
                success_sha="abc1234",
                success_time="2026-06-10T01:00:00Z",
                success_log_url="https://console.cloud.google.com/cloud-build/builds/ok",
            )
        }
        signal = repo_ci._image_signal(view, "execution-service", main_sha="abc1234", builds=builds)  # pyright: ignore[reportPrivateUsage]
        assert signal["last_build_status"] == "FAILURE"
        assert signal["last_build_sha"] == "def5678"  # latest (failed) build sha still shown
        assert signal["last_success_sha"] == "abc1234"  # the last GOOD sha is recoverable
        assert signal["last_success_time"] == "2026-06-10T01:00:00Z"
        assert signal["last_success_log_url"] == "https://console.cloud.google.com/cloud-build/builds/ok"
        # main HEAD == last-success sha → the running image is up to date despite the failed latest.
        assert signal["image_stale"] is False


class TestRecentBuildsByRepoName:
    """The Image-column fix: GCP builds are matched to repos by the REPO_NAME substitution
    (newest build wins), NOT the build_trigger_id filter (which the regional list_builds API
    400-rejects → swallowed → empty Image column)."""

    async def test_groups_by_repo_name_keeping_newest(self) -> None:
        from deployment_api.routes import _cloud_builds_history as hist

        class _Subs:
            def __init__(self, repo: str | None) -> None:
                self._repo = repo

            def get(self, key: str) -> str | None:
                return self._repo if key == "REPO_NAME" else None

        class _Build:
            def __init__(self, repo: str | None, status: str = "SUCCESS", sha: str = "s") -> None:
                self.substitutions = _Subs(repo)
                self.status = status
                self.sha = sha

        # list_builds yields newest-first; per repo keep first (latest) + first SUCCESS (last good).
        # mtds: latest FAILURE then an earlier SUCCESS → last-success differs from latest.
        seq = [
            _Build("mtds", "FAILURE", "mtds-new"),
            _Build("uac", "SUCCESS", "uac-new"),
            _Build("mtds", "SUCCESS", "mtds-old"),
            _Build(""),
            _Build(None),
        ]

        class _Client:
            def list_builds(self, request: object) -> object:
                _ = request
                return iter(seq)

        class _CbModule:
            def ListBuildsRequest(self, **kwargs: object) -> object:  # noqa: N802 (mirrors proto name)
                _ = kwargs
                return object()

        def _fake_format(build: object) -> dict[str, str | None]:
            return {"status": build.status, "commit_sha": build.sha, "finish_time": None, "log_url": None}  # type: ignore[attr-defined]

        with (
            patch.object(hist, "get_cloudbuild_v1", return_value=_CbModule()),
            patch.object(hist, "get_gcp_build_client", return_value=_Client()),
            patch.object(hist, "_format_build_info", side_effect=_fake_format),
        ):
            result = await hist._recent_builds_by_repo_name()  # pyright: ignore[reportPrivateUsage]

        # 5 builds → 2 repos (blank/None REPO_NAME dropped); each value = (latest, last_success).
        assert set(result) == {"mtds", "uac"}
        mtds_latest, mtds_success = result["mtds"]
        assert mtds_latest["status"] == "FAILURE" and mtds_latest["commit_sha"] == "mtds-new"
        assert mtds_success is not None and mtds_success["commit_sha"] == "mtds-old"  # last green behind the red latest
        uac_latest, uac_success = result["uac"]
        assert (
            uac_latest["commit_sha"] == "uac-new" and uac_success is not None and uac_success["commit_sha"] == "uac-new"
        )
