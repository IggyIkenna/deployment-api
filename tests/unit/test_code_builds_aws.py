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


class TestWifCodebuildClient:
    """Keyless GCP->AWS WIF auth for the CodeBuild reader (Option B): with a role ARN configured the
    client is built from short-lived AssumeRoleWithWebIdentity creds (no static AWS key anywhere);
    without one the default boto3 credential chain is used; assumed creds are cached ~50min."""

    def _reset_cache(self) -> None:
        import deployment_api.routes._code_builds_aws as mod

        mod._wif_creds_cache = None  # pyright: ignore[reportPrivateUsage]

    def test_role_arn_set_assumes_role_keyless(self) -> None:
        import deployment_api.routes._code_builds_aws as mod

        self._reset_cache()
        fake_sts = MagicMock()
        fake_sts.assume_role_with_web_identity.return_value = {
            "Credentials": {
                "AccessKeyId": "AKIA_TMP",
                "SecretAccessKey": "SECRET_TMP",
                "SessionToken": "TOKEN_TMP",
            }
        }
        fake_codebuild = MagicMock()
        fake_session = MagicMock()
        fake_session.client.return_value = fake_codebuild
        role_arn = "arn:aws:iam::427895769566:role/gcp-cloudrun-codebuild-reader"

        with (
            patch.object(mod, "AWS_CODEBUILD_READER_ROLE_ARN", role_arn),
            patch("boto3.client", return_value=fake_sts) as boto_client,
            patch("boto3.Session", return_value=fake_session) as session_ctor,
            patch("google.oauth2.id_token.fetch_id_token", return_value="google-oidc-token"),
            patch("google.auth.transport.requests.Request", return_value=MagicMock()),
        ):
            client = mod._get_codebuild_client()  # pyright: ignore[reportPrivateUsage]
            self._reset_cache()

        # STS client built for the unsigned web-identity call; the role assumed with the OIDC token.
        assert boto_client.call_args.args[0] == "sts"
        fake_sts.assume_role_with_web_identity.assert_called_once()
        kw = fake_sts.assume_role_with_web_identity.call_args.kwargs
        assert kw["RoleArn"] == role_arn
        assert kw["WebIdentityToken"] == "google-oidc-token"
        assert kw["RoleSessionName"] == "repo-ci-codebuild"
        # Session built from the SHORT-LIVED assumed creds — never a stored static key.
        session_ctor.assert_called_once_with(
            aws_access_key_id="AKIA_TMP",
            aws_secret_access_key="SECRET_TMP",
            aws_session_token="TOKEN_TMP",
        )
        assert client is fake_codebuild

    def test_role_arn_unset_uses_default_chain(self) -> None:
        import deployment_api.routes._code_builds_aws as mod

        self._reset_cache()
        fake_codebuild = MagicMock()
        with (
            patch.object(mod, "AWS_CODEBUILD_READER_ROLE_ARN", ""),
            patch("boto3.client", return_value=fake_codebuild) as boto_client,
            patch("boto3.Session") as session_ctor,
        ):
            client = mod._get_codebuild_client()  # pyright: ignore[reportPrivateUsage]

        # Default chain: a plain codebuild client, no WIF session.
        boto_client.assert_called_once()
        assert boto_client.call_args.args[0] == "codebuild"
        session_ctor.assert_not_called()
        assert client is fake_codebuild

    def test_wif_creds_cached_no_reassume_on_second_call(self) -> None:
        import deployment_api.routes._code_builds_aws as mod

        self._reset_cache()
        fake_sts = MagicMock()
        fake_sts.assume_role_with_web_identity.return_value = {
            "Credentials": {"AccessKeyId": "A", "SecretAccessKey": "S", "SessionToken": "T"}
        }
        with (
            patch.object(mod, "AWS_CODEBUILD_READER_ROLE_ARN", "arn:aws:iam::1:role/r"),
            patch("boto3.client", return_value=fake_sts),
            patch("boto3.Session", return_value=MagicMock()),
            patch("google.oauth2.id_token.fetch_id_token", return_value="tok"),
            patch("google.auth.transport.requests.Request", return_value=MagicMock()),
        ):
            mod._get_codebuild_client()  # pyright: ignore[reportPrivateUsage]
            mod._get_codebuild_client()  # pyright: ignore[reportPrivateUsage]
            self._reset_cache()

        # The role is assumed ONCE; the second client reuses the cached short-lived creds.
        assert fake_sts.assume_role_with_web_identity.call_count == 1
