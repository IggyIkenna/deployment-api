"""
Unit tests for AWS CodeBuild client helpers (_code_builds_aws.py).

Tests cover:
- _format_codebuild_build (status mapping, field extraction)
- is_aws_provider
- list_codebuild_projects_sync (with mocked boto3)
- get_codebuild_history_sync (with mocked boto3)
- start_codebuild_sync (with mocked boto3)
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from deployment_api.routes._code_builds_aws import (
    _format_codebuild_build,
    get_codebuild_history_sync,
    get_recent_builds_for_projects_sync,
    is_aws_provider,
    list_codebuild_projects_sync,
    start_codebuild_sync,
)


class TestFormatCodebuildBuild:
    """Tests for _format_codebuild_build."""

    def test_succeeded_build(self) -> None:
        start = datetime(2026, 4, 1, 10, 0, 0, tzinfo=UTC)
        end = datetime(2026, 4, 1, 10, 8, 30, tzinfo=UTC)
        build: dict[str, object] = {
            "id": "instruments-service-build:abc-123",
            "buildStatus": "SUCCEEDED",
            "startTime": start,
            "endTime": end,
            "sourceVersion": "live-defi-rollout",
            "resolvedSourceVersion": "7263d34abcdef1234567890",
            "projectName": "instruments-service-build",
            "logs": {"deepLink": "https://logs.example.com/build/abc-123"},
        }
        result = _format_codebuild_build(build)
        assert result["status"] == "SUCCESS"
        assert result["build_id"] == "abc-123"
        assert result["duration_seconds"] == 510.0
        assert result["branch"] == "live-defi-rollout"
        assert result["commit_sha"] == "7263d34"
        assert result["log_url"] == "https://logs.example.com/build/abc-123"

    def test_failed_build(self) -> None:
        build: dict[str, object] = {
            "id": "svc:xyz-456",
            "buildStatus": "FAILED",
            "projectName": "svc",
        }
        result = _format_codebuild_build(build)
        assert result["status"] == "FAILURE"
        assert result["build_id"] == "xyz-456"
        assert result["duration_seconds"] is None

    def test_in_progress_build(self) -> None:
        build: dict[str, object] = {
            "id": "svc:in-prog",
            "buildStatus": "IN_PROGRESS",
            "projectName": "svc",
        }
        result = _format_codebuild_build(build)
        assert result["status"] == "WORKING"

    def test_timed_out_build(self) -> None:
        build: dict[str, object] = {
            "id": "svc:timeout",
            "buildStatus": "TIMED_OUT",
            "projectName": "svc",
        }
        result = _format_codebuild_build(build)
        assert result["status"] == "TIMEOUT"

    def test_fallback_log_url(self) -> None:
        build: dict[str, object] = {
            "id": "myproj:build-001",
            "buildStatus": "SUCCEEDED",
            "projectName": "myproj",
        }
        result = _format_codebuild_build(build)
        assert "myproj" in str(result["log_url"])
        assert "build-001" in str(result["log_url"]) or "myproj:build-001" in str(result["log_url"])

    def test_no_source_version(self) -> None:
        build: dict[str, object] = {
            "id": "svc:no-src",
            "buildStatus": "SUCCEEDED",
            "projectName": "svc",
        }
        result = _format_codebuild_build(build)
        assert result["branch"] is None
        assert result["commit_sha"] is None


class TestIsAwsProvider:
    """Tests for is_aws_provider."""

    def test_is_aws(self) -> None:
        with patch("deployment_api.routes._code_builds_aws.CLOUD_PROVIDER", "aws"):
            assert is_aws_provider() is True

    def test_is_gcp(self) -> None:
        with patch("deployment_api.routes._code_builds_aws.CLOUD_PROVIDER", "gcp"):
            assert is_aws_provider() is False

    def test_is_local(self) -> None:
        with patch("deployment_api.routes._code_builds_aws.CLOUD_PROVIDER", "local"):
            assert is_aws_provider() is False


class TestListCodebuildProjects:
    """Tests for list_codebuild_projects_sync."""

    def test_returns_matching_projects(self) -> None:
        mock_client = MagicMock()
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {
                "projects": [
                    "instruments-service-build",
                    "unrelated-project",
                    "execution-service-build",
                ]
            }
        ]
        mock_client.get_paginator.return_value = mock_paginator
        mock_client.batch_get_projects.return_value = {
            "projects": [
                {
                    "name": "instruments-service-build",
                    "source": {
                        "type": "GITHUB",
                        "location": "https://github.com/IggyIkenna/instruments-service.git",
                    },
                },
                {
                    "name": "execution-service-build",
                    "source": {
                        "type": "GITHUB",
                        "location": "https://github.com/IggyIkenna/execution-service.git",
                    },
                },
            ]
        }

        with patch("deployment_api.routes._code_builds_aws._get_codebuild_client", return_value=mock_client):
            triggers = list_codebuild_projects_sync()

        assert len(triggers) == 2
        assert triggers[0]["trigger_id"] == "instruments-service-build"
        assert triggers[0]["service"] == "instruments-service"
        assert triggers[0]["github_repo"] == "IggyIkenna/instruments-service"

    def test_no_matching_projects(self) -> None:
        mock_client = MagicMock()
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [{"projects": ["unrelated-project"]}]
        mock_client.get_paginator.return_value = mock_paginator

        with patch("deployment_api.routes._code_builds_aws._get_codebuild_client", return_value=mock_client):
            triggers = list_codebuild_projects_sync()

        assert triggers == []


class TestGetCodebuildHistory:
    """Tests for get_codebuild_history_sync."""

    def test_returns_builds(self) -> None:
        mock_client = MagicMock()
        mock_client.list_builds_for_project.return_value = {"ids": ["proj:build-1", "proj:build-2"]}
        mock_client.batch_get_builds.return_value = {
            "builds": [
                {"id": "proj:build-1", "buildStatus": "SUCCEEDED", "projectName": "proj"},
                {"id": "proj:build-2", "buildStatus": "FAILED", "projectName": "proj"},
            ]
        }

        with patch("deployment_api.routes._code_builds_aws._get_codebuild_client", return_value=mock_client):
            history = get_codebuild_history_sync("proj", limit=5)

        assert len(history) == 2
        assert history[0]["status"] == "SUCCESS"
        assert history[1]["status"] == "FAILURE"

    def test_empty_builds(self) -> None:
        mock_client = MagicMock()
        mock_client.list_builds_for_project.return_value = {"ids": []}

        with patch("deployment_api.routes._code_builds_aws._get_codebuild_client", return_value=mock_client):
            history = get_codebuild_history_sync("proj")

        assert history == []


class TestGetRecentBuildsForProjects:
    """Tests for get_recent_builds_for_projects_sync."""

    def test_returns_latest_per_project(self) -> None:
        mock_client = MagicMock()
        mock_client.list_builds_for_project.return_value = {"ids": ["proj:latest"]}
        mock_client.batch_get_builds.return_value = {
            "builds": [{"id": "proj:latest", "buildStatus": "SUCCEEDED", "projectName": "proj"}]
        }

        with patch("deployment_api.routes._code_builds_aws._get_codebuild_client", return_value=mock_client):
            result = get_recent_builds_for_projects_sync(["proj"])

        assert "proj" in result
        assert result["proj"] is not None
        assert result["proj"]["status"] == "SUCCESS"

    def test_handles_missing_project(self) -> None:
        mock_client = MagicMock()
        mock_client.list_builds_for_project.side_effect = Exception("ProjectNotFound")

        with patch("deployment_api.routes._code_builds_aws._get_codebuild_client", return_value=mock_client):
            result = get_recent_builds_for_projects_sync(["nonexistent"])

        assert result["nonexistent"] is None


class TestStartCodebuild:
    """Tests for start_codebuild_sync."""

    def test_starts_build(self) -> None:
        mock_client = MagicMock()
        mock_client.start_build.return_value = {
            "build": {
                "id": "instruments-service-build:new-build-123",
                "buildStatus": "IN_PROGRESS",
            }
        }

        with patch("deployment_api.routes._code_builds_aws._get_codebuild_client", return_value=mock_client):
            result = start_codebuild_sync("instruments-service-build", "live-defi-rollout")

        assert result["build_id"] == "new-build-123"
        assert "instruments-service-build" in str(result["log_url"])
        mock_client.start_build.assert_called_once_with(
            projectName="instruments-service-build",
            sourceVersion="live-defi-rollout",
        )
