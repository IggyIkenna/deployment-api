"""Unit tests for deployment_api/routes/_cloud_builds_trigger.py helpers."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

# ── _resolve_trigger_repo ─────────────────────────────────────────────────────


class TestResolveTriggerRepo:
    def test_known_service_returns_service_type(self) -> None:
        from deployment_api.routes._cloud_builds_trigger import _resolve_trigger_repo
        from deployment_api.routes._cloud_builds_types import SERVICES_WITH_TRIGGERS

        if not SERVICES_WITH_TRIGGERS:
            return
        service = next(iter(SERVICES_WITH_TRIGGERS))
        trigger = MagicMock()
        trigger.name = f"{service}-build"
        name, kind = _resolve_trigger_repo(trigger)
        assert name == service
        assert kind == "service"

    def test_known_library_returns_library_type(self) -> None:
        from deployment_api.routes._cloud_builds_trigger import _resolve_trigger_repo
        from deployment_api.routes._cloud_builds_types import LIBRARIES_WITH_TRIGGERS

        if not LIBRARIES_WITH_TRIGGERS:
            return
        lib = next(iter(LIBRARIES_WITH_TRIGGERS))
        trigger = MagicMock()
        trigger.name = f"{lib}-build"
        name, kind = _resolve_trigger_repo(trigger)
        assert name == lib
        assert kind == "library"

    def test_known_infrastructure_returns_infra_type(self) -> None:
        from deployment_api.routes._cloud_builds_trigger import _resolve_trigger_repo
        from deployment_api.routes._cloud_builds_types import INFRASTRUCTURE_WITH_TRIGGERS

        if not INFRASTRUCTURE_WITH_TRIGGERS:
            return
        infra = next(iter(INFRASTRUCTURE_WITH_TRIGGERS))
        trigger = MagicMock()
        trigger.name = f"{infra}-build"
        name, kind = _resolve_trigger_repo(trigger)
        assert name == infra
        assert kind == "infrastructure"

    def test_unknown_trigger_returns_none_none(self) -> None:
        from deployment_api.routes._cloud_builds_trigger import _resolve_trigger_repo

        trigger = MagicMock()
        trigger.name = "unknown-service-xyz-build"
        name, kind = _resolve_trigger_repo(trigger)
        assert name is None
        assert kind is None

    def test_empty_trigger_name_returns_none(self) -> None:
        from deployment_api.routes._cloud_builds_trigger import _resolve_trigger_repo

        trigger = MagicMock()
        trigger.name = ""
        name, kind = _resolve_trigger_repo(trigger)
        assert name is None
        assert kind is None


# ── _extract_github_info ──────────────────────────────────────────────────────


class TestExtractGithubInfo:
    def test_github_with_push_branch_returns_repo_and_branch(self) -> None:
        from deployment_api.routes._cloud_builds_trigger import _extract_github_info

        push = MagicMock()
        push.branch = "main"
        github = MagicMock()
        github.owner = "IggyIkenna"
        github.name = "execution-service"
        github.push = push
        trigger = MagicMock()
        trigger.github = github
        trigger.repository_event_config = None
        repo, branch = _extract_github_info(trigger)
        assert repo == "IggyIkenna/execution-service"
        assert branch == "main"

    def test_github_without_push_returns_repo_none_branch(self) -> None:
        from deployment_api.routes._cloud_builds_trigger import _extract_github_info

        github = MagicMock()
        github.owner = "IggyIkenna"
        github.name = "strategy-service"
        github.push = None
        trigger = MagicMock()
        trigger.github = github
        trigger.repository_event_config = None
        repo, branch = _extract_github_info(trigger)
        assert repo == "IggyIkenna/strategy-service"
        assert branch is None

    def test_repo_event_config_returns_short_name(self) -> None:
        from deployment_api.routes._cloud_builds_trigger import _extract_github_info

        push = MagicMock()
        push.branch = "staging"
        repo_event = MagicMock()
        repo_event.repository = "projects/my-proj/repos/execution-service"
        repo_event.push = push
        trigger = MagicMock()
        trigger.github = None
        trigger.repository_event_config = repo_event
        repo, branch = _extract_github_info(trigger)
        assert repo == "execution-service"
        assert branch == "staging"

    def test_no_github_no_repo_event_returns_none_none(self) -> None:
        from deployment_api.routes._cloud_builds_trigger import _extract_github_info

        trigger = MagicMock()
        trigger.github = None
        trigger.repository_event_config = None
        repo, branch = _extract_github_info(trigger)
        assert repo is None
        assert branch is None


# ── _extract_build_id_from_op ─────────────────────────────────────────────────


class TestExtractBuildIdFromOp:
    def test_op_name_with_build_id_parses_id(self) -> None:
        from deployment_api.routes._cloud_builds_trigger import _extract_build_id_from_op

        op = MagicMock()
        op.metadata = None
        op_name = "projects/proj/locations/us-central1/operations/abc12345-6789-abcd-ef01-234567890123"
        build_id, log_url = _extract_build_id_from_op(op_name, op)
        assert build_id == "abc12345-6789-abcd-ef01-234567890123"
        assert log_url is None

    def test_op_name_short_not_parsed(self) -> None:
        from deployment_api.routes._cloud_builds_trigger import _extract_build_id_from_op

        op = MagicMock()
        op.metadata = None
        op_name = "projects/proj/locations/us-central1/operations/shortid"
        build_id, log_url = _extract_build_id_from_op(op_name, op)
        assert build_id is None

    def test_none_op_name_returns_none(self) -> None:
        from deployment_api.routes._cloud_builds_trigger import _extract_build_id_from_op

        op = MagicMock()
        op.metadata = None
        build_id, log_url = _extract_build_id_from_op(None, op)
        assert build_id is None
        assert log_url is None

    def test_metadata_unpack_error_falls_through_to_op_name(self) -> None:
        from deployment_api.routes._cloud_builds_trigger import _extract_build_id_from_op

        op = MagicMock()
        op.metadata = MagicMock()
        with patch("deployment_api.routes._cloud_builds_types._build_op_meta_cls") as mock_cls:
            mock_cls.return_value = MagicMock(side_effect=RuntimeError("unpack failed"))
            op_name = "projects/proj/locations/us-central1/operations/abc12345-6789-abcd-ef01-234567890123"
            build_id, log_url = _extract_build_id_from_op(op_name, op)
        # Falls through to op_name parse
        assert build_id == "abc12345-6789-abcd-ef01-234567890123"


# ── _build_trigger_list_sync ──────────────────────────────────────────────────


class TestBuildTriggerListSync:
    def test_returns_list_with_known_service_triggers(self) -> None:
        from deployment_api.routes._cloud_builds_trigger import _build_trigger_list_sync
        from deployment_api.routes._cloud_builds_types import SERVICES_WITH_TRIGGERS

        if not SERVICES_WITH_TRIGGERS:
            return
        service = next(iter(SERVICES_WITH_TRIGGERS))

        mock_trigger = MagicMock()
        mock_trigger.name = f"{service}-build"
        mock_trigger.id = "trigger-id-123"
        mock_trigger.disabled = False
        mock_trigger.github = MagicMock()
        mock_trigger.github.owner = "IggyIkenna"
        mock_trigger.github.name = service
        mock_trigger.github.push = MagicMock()
        mock_trigger.github.push.branch = "main"
        mock_trigger.repository_event_config = None

        mock_client = MagicMock()
        mock_client.list_build_triggers.return_value = [mock_trigger]
        mock_cb = MagicMock()
        mock_cb.ListBuildTriggersRequest.return_value = MagicMock()

        with (
            patch("deployment_api.routes._cloud_builds_trigger._get_gcp_build_client", return_value=mock_client),
            patch("deployment_api.routes._cloud_builds_trigger._cloudbuild_v1", return_value=mock_cb),
        ):
            result = _build_trigger_list_sync()
        assert len(result) == 1
        assert result[0]["service"] == service
        assert result[0]["trigger_id"] == "trigger-id-123"

    def test_unknown_triggers_are_skipped(self) -> None:
        from deployment_api.routes._cloud_builds_trigger import _build_trigger_list_sync

        mock_trigger = MagicMock()
        mock_trigger.name = "unknown-service-xyz-build"
        mock_trigger.id = "trigger-skip-1"

        mock_client = MagicMock()
        mock_client.list_build_triggers.return_value = [mock_trigger]
        mock_cb = MagicMock()
        mock_cb.ListBuildTriggersRequest.return_value = MagicMock()

        with (
            patch("deployment_api.routes._cloud_builds_trigger._get_gcp_build_client", return_value=mock_client),
            patch("deployment_api.routes._cloud_builds_trigger._cloudbuild_v1", return_value=mock_cb),
        ):
            result = _build_trigger_list_sync()
        assert result == []


# ── _get_trigger_id_sync ──────────────────────────────────────────────────────


class TestGetTriggerIdSync:
    def test_returns_cached_id_without_api_call(self) -> None:
        import time

        import deployment_api.routes._cloud_builds_trigger as _mod

        _mod._trigger_id_cache = {"my-service-build": "cached-id-999"}
        _mod._trigger_cache_time = time.time()

        result = _mod._get_trigger_id_sync("my-service-build")
        assert result == "cached-id-999"

    def test_expired_cache_calls_api(self) -> None:
        import deployment_api.routes._cloud_builds_trigger as _mod

        _mod._trigger_id_cache = {}
        _mod._trigger_cache_time = 0  # Force expiry

        mock_trigger = MagicMock()
        mock_trigger.name = "my-service-build"
        mock_trigger.id = "api-trigger-id-42"
        mock_client = MagicMock()
        mock_client.list_build_triggers.return_value = [mock_trigger]
        mock_cb = MagicMock()
        mock_cb.ListBuildTriggersRequest.return_value = MagicMock()

        with (
            patch("deployment_api.routes._cloud_builds_trigger._get_gcp_build_client", return_value=mock_client),
            patch("deployment_api.routes._cloud_builds_trigger._cloudbuild_v1", return_value=mock_cb),
        ):
            result = _mod._get_trigger_id_sync("my-service-build")
        assert result == "api-trigger-id-42"


# ── _find_recent_build_sync ───────────────────────────────────────────────────


class TestFindRecentBuildSync:
    def test_build_after_started_returns_build(self) -> None:
        from deployment_api.routes._cloud_builds_trigger import _find_recent_build_sync

        started = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
        build = MagicMock()
        build.create_time = datetime(2026, 5, 1, 12, 30, 0, tzinfo=UTC)
        build.id = "build-abc-123"
        build.log_url = "https://console.cloud.google.com/builds/abc"
        build.status.name = "SUCCESS"

        mock_client = MagicMock()
        mock_client.list_builds.return_value = [build]
        mock_cb = MagicMock()
        mock_cb.ListBuildsRequest.return_value = MagicMock()

        with (
            patch("deployment_api.routes._cloud_builds_trigger._get_gcp_build_client", return_value=mock_client),
            patch("deployment_api.routes._cloud_builds_trigger._cloudbuild_v1", return_value=mock_cb),
        ):
            result = _find_recent_build_sync("trigger-id-1", started)
        assert result is not None
        assert result["build_id"] == "build-abc-123"
        assert result["status"] == "SUCCESS"

    def test_build_before_started_returns_none(self) -> None:
        from deployment_api.routes._cloud_builds_trigger import _find_recent_build_sync

        started = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)
        build = MagicMock()
        build.create_time = datetime(2026, 5, 1, 11, 0, 0, tzinfo=UTC)

        mock_client = MagicMock()
        mock_client.list_builds.return_value = [build]
        mock_cb = MagicMock()
        mock_cb.ListBuildsRequest.return_value = MagicMock()

        with (
            patch("deployment_api.routes._cloud_builds_trigger._get_gcp_build_client", return_value=mock_client),
            patch("deployment_api.routes._cloud_builds_trigger._cloudbuild_v1", return_value=mock_cb),
        ):
            result = _find_recent_build_sync("trigger-id-1", started)
        assert result is None

    def test_empty_builds_returns_none(self) -> None:
        from deployment_api.routes._cloud_builds_trigger import _find_recent_build_sync

        mock_client = MagicMock()
        mock_client.list_builds.return_value = []
        mock_cb = MagicMock()
        mock_cb.ListBuildsRequest.return_value = MagicMock()

        with (
            patch("deployment_api.routes._cloud_builds_trigger._get_gcp_build_client", return_value=mock_client),
            patch("deployment_api.routes._cloud_builds_trigger._cloudbuild_v1", return_value=mock_cb),
        ):
            result = _find_recent_build_sync("trigger-id-1", datetime.now(UTC))
        assert result is None


# ── _run_trigger_operation_sync ───────────────────────────────────────────────


class TestRunTriggerOperationSync:
    def test_successful_run_returns_result_with_trigger_id(self) -> None:
        import deployment_api.routes._cloud_builds_trigger as _mod

        _mod._trigger_id_cache = {"execution-service-build": "trigger-exec-1"}
        _mod._trigger_cache_time = __import__("time").time()

        op = MagicMock()
        op.name = "projects/proj/locations/us-central1/operations/abc12345-6789-abcd-ef01-234567890123"
        op.done = True
        op.metadata = None

        mock_client = MagicMock()
        mock_client.run_build_trigger.return_value = op
        mock_cb = MagicMock()
        mock_cb.RunBuildTriggerRequest.return_value = MagicMock()
        mock_cb.RepoSource.return_value = MagicMock()

        with (
            patch("deployment_api.routes._cloud_builds_trigger._get_gcp_build_client", return_value=mock_client),
            patch("deployment_api.routes._cloud_builds_trigger._cloudbuild_v1", return_value=mock_cb),
        ):
            result = _mod._run_trigger_operation_sync("execution-service-build", "main")
        assert result["success"] is True
        assert result["trigger_id"] == "trigger-exec-1"
        assert result["build_id"] == "abc12345-6789-abcd-ef01-234567890123"
