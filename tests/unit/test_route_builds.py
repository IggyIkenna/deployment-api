"""Unit tests for routes/builds.py — pure helpers + mock-mode route coverage."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unified_trading_library import setup_events

setup_events("deployment-api", "test")

_PATCH_CLOUD_MOCK_MODE = "deployment_api.routes.builds.CLOUD_MOCK_MODE"
_PATCH_CLOUD_PROVIDER = "deployment_api.routes.builds.CLOUD_PROVIDER"
_PATCH_PROJECT_ID = "deployment_api.routes.builds.default_project_id"
_PATCH_WORKSPACE_ROOT = "deployment_api.routes.builds.WORKSPACE_ROOT"
_PATCH_DISABLE_AUTH = "deployment_api.rbac.DISABLE_AUTH"


# ── _tag_to_entry ─────────────────────────────────────────────────────────────


class TestTagToEntry:
    def test_semver_main_branch(self) -> None:
        from deployment_api.routes.builds import _tag_to_entry

        entry = _tag_to_entry("1.2.3")
        assert entry.tag == "1.2.3"
        assert entry.branch == "main"
        assert entry.version == "1.2.3"
        assert entry.display == "1.2.3 @ main"
        assert entry.is_v1 is True

    def test_semver_staging_branch(self) -> None:
        from deployment_api.routes.builds import _tag_to_entry

        entry = _tag_to_entry("0.3.168-staging")
        assert entry.branch == "staging"
        assert entry.display == "0.3.168 @ staging"
        assert entry.is_v1 is False

    def test_semver_feat_branch(self) -> None:
        from deployment_api.routes.builds import _tag_to_entry

        entry = _tag_to_entry("0.3.168-feat-my-feature")
        assert entry.branch == "feat/my-feature"
        assert entry.display == "0.3.168 @ feat/my-feature"

    def test_semver_fix_branch(self) -> None:
        from deployment_api.routes.builds import _tag_to_entry

        entry = _tag_to_entry("1.0.0-fix-the-bug")
        assert entry.branch == "fix/the-bug"

    def test_semver_chore_branch(self) -> None:
        from deployment_api.routes.builds import _tag_to_entry

        entry = _tag_to_entry("0.5.0-chore-cleanup")
        assert entry.branch == "chore/cleanup"

    def test_commit_sha_short(self) -> None:
        from deployment_api.routes.builds import _tag_to_entry

        entry = _tag_to_entry("3c1f37a")
        assert entry.tag == "3c1f37a"
        assert entry.branch == "commit"
        assert entry.display == "3c1f37a @ commit"
        assert entry.is_v1 is False

    def test_commit_sha_long(self) -> None:
        from deployment_api.routes.builds import _tag_to_entry

        entry = _tag_to_entry("abc1234def5678ab")
        assert entry.branch == "commit"

    def test_non_semver_unknown_tag(self) -> None:
        from deployment_api.routes.builds import _tag_to_entry

        entry = _tag_to_entry("not-a-version")
        assert entry.tag == "not-a-version"
        assert entry.branch == "unknown"
        assert entry.is_v1 is False

    def test_v1_major_version(self) -> None:
        from deployment_api.routes.builds import _tag_to_entry

        entry = _tag_to_entry("2.0.0")
        assert entry.is_v1 is True

    def test_v0_is_not_v1(self) -> None:
        from deployment_api.routes.builds import _tag_to_entry

        entry = _tag_to_entry("0.9.99")
        assert entry.is_v1 is False

    def test_feat_branch_with_dashes_in_name(self) -> None:
        from deployment_api.routes.builds import _tag_to_entry

        entry = _tag_to_entry("0.1.0-feat-live-defi-rollout")
        assert entry.branch == "feat/live-defi-rollout"


# ── _sort_key ────────────────────────────────────────────────────────────────


class TestSortKey:
    def test_higher_major_sorts_first(self) -> None:
        from deployment_api.routes.builds import BuildEntry, _sort_key

        e1 = BuildEntry(tag="2.0.0", display="2.0.0 @ main", version="2.0.0", branch="main", is_v1=True)
        e2 = BuildEntry(tag="1.0.0", display="1.0.0 @ main", version="1.0.0", branch="main", is_v1=True)
        assert _sort_key(e1) < _sort_key(e2)

    def test_higher_minor_sorts_first(self) -> None:
        from deployment_api.routes.builds import BuildEntry, _sort_key

        e1 = BuildEntry(tag="1.2.0", display="1.2.0 @ main", version="1.2.0", branch="main", is_v1=True)
        e2 = BuildEntry(tag="1.1.0", display="1.1.0 @ main", version="1.1.0", branch="main", is_v1=True)
        assert _sort_key(e1) < _sort_key(e2)

    def test_higher_patch_sorts_first(self) -> None:
        from deployment_api.routes.builds import BuildEntry, _sort_key

        e1 = BuildEntry(tag="1.0.3", display="1.0.3 @ main", version="1.0.3", branch="main", is_v1=True)
        e2 = BuildEntry(tag="1.0.1", display="1.0.1 @ main", version="1.0.1", branch="main", is_v1=True)
        assert _sort_key(e1) < _sort_key(e2)

    def test_invalid_version_returns_zero_tuple(self) -> None:
        from deployment_api.routes.builds import BuildEntry, _sort_key

        e = BuildEntry(tag="abc", display="abc @ commit", version="abc", branch="commit", is_v1=False)
        assert _sort_key(e) == (0, 0, 0)


# ── _mock_builds_from_manifest ───────────────────────────────────────────────


class TestMockBuildsFromManifest:
    def test_missing_manifest_returns_empty(self, tmp_path: Path) -> None:
        from deployment_api.routes.builds import _mock_builds_from_manifest

        with patch(_PATCH_WORKSPACE_ROOT, str(tmp_path)):
            result = _mock_builds_from_manifest("my-service", "dev")
        assert result == []

    def test_manifest_with_deployed_version_returns_entry(self, tmp_path: Path) -> None:
        import json

        from deployment_api.routes.builds import _mock_builds_from_manifest

        pm_dir = tmp_path / "unified-trading-pm"
        pm_dir.mkdir(parents=True)
        manifest = {"deployed_versions": {"dev": {"my-service": "1.0.0"}}}
        (pm_dir / "workspace-manifest.json").write_text(json.dumps(manifest))

        with patch(_PATCH_WORKSPACE_ROOT, str(tmp_path)):
            result = _mock_builds_from_manifest("my-service", "dev")

        assert len(result) == 1
        assert result[0].tag == "1.0.0"

    def test_manifest_with_missing_env_returns_empty(self, tmp_path: Path) -> None:
        import json

        from deployment_api.routes.builds import _mock_builds_from_manifest

        pm_dir = tmp_path / "unified-trading-pm"
        pm_dir.mkdir(parents=True)
        manifest = {"deployed_versions": {"prod": {"my-service": "1.0.0"}}}
        (pm_dir / "workspace-manifest.json").write_text(json.dumps(manifest))

        with patch(_PATCH_WORKSPACE_ROOT, str(tmp_path)):
            result = _mock_builds_from_manifest("my-service", "dev")

        assert result == []

    def test_invalid_json_returns_empty(self, tmp_path: Path) -> None:
        from deployment_api.routes.builds import _mock_builds_from_manifest

        pm_dir = tmp_path / "unified-trading-pm"
        pm_dir.mkdir(parents=True)
        (pm_dir / "workspace-manifest.json").write_text("not-json!")

        with patch(_PATCH_WORKSPACE_ROOT, str(tmp_path)):
            result = _mock_builds_from_manifest("my-service", "dev")

        assert result == []


# ── _get_ar_repo_name ────────────────────────────────────────────────────────


class TestGetArRepoName:
    def test_instruments_service_override(self) -> None:
        from deployment_api.routes.builds import _get_ar_repo_name

        assert _get_ar_repo_name("instruments-service") == "instruments"

    def test_execution_service_override(self) -> None:
        from deployment_api.routes.builds import _get_ar_repo_name

        assert _get_ar_repo_name("execution-service") == "execution"

    def test_unlisted_service_defaults_to_cb_repo(self) -> None:
        from deployment_api.routes.builds import _get_ar_repo_name

        assert _get_ar_repo_name("my-custom-service") == "unified-trading-system"


# ── list_builds route (mock mode) ─────────────────────────────────────────────


@pytest.fixture
def client_builds() -> TestClient:
    from deployment_api.routes.builds import router

    app = FastAPI()
    app.include_router(router)
    with patch(_PATCH_DISABLE_AUTH, True):
        return TestClient(app, raise_server_exceptions=False)


def test_list_builds_mock_mode_empty_manifest(client_builds: TestClient, tmp_path: Path) -> None:
    with (
        patch(_PATCH_CLOUD_MOCK_MODE, True),
        patch(_PATCH_WORKSPACE_ROOT, str(tmp_path)),
    ):
        r = client_builds.get("/api/builds/my-service?env=dev")
    assert r.status_code == 200
    assert r.json() == []


def test_list_builds_mock_mode_with_manifest_entry(client_builds: TestClient, tmp_path: Path) -> None:
    import json

    pm_dir = tmp_path / "unified-trading-pm"
    pm_dir.mkdir(parents=True)
    manifest = {"deployed_versions": {"dev": {"my-service": "1.2.3"}}}
    (pm_dir / "workspace-manifest.json").write_text(json.dumps(manifest))

    with (
        patch(_PATCH_CLOUD_MOCK_MODE, True),
        patch(_PATCH_WORKSPACE_ROOT, str(tmp_path)),
    ):
        r = client_builds.get("/api/builds/my-service?env=dev")

    assert r.status_code == 200
    data = r.json()
    assert len(data) == 1
    assert data[0]["tag"] == "1.2.3"
    assert data[0]["branch"] == "main"


def test_list_builds_local_provider_also_mock(client_builds: TestClient, tmp_path: Path) -> None:
    with (
        patch(_PATCH_CLOUD_MOCK_MODE, False),
        patch(_PATCH_CLOUD_PROVIDER, "local"),
        patch(_PATCH_WORKSPACE_ROOT, str(tmp_path)),
    ):
        r = client_builds.get("/api/builds/my-service?env=staging")
    assert r.status_code == 200


# ── deploy_build route (mock mode) ────────────────────────────────────────────


def test_deploy_build_mock_mode_returns_accepted(client_builds: TestClient) -> None:
    with (
        patch(_PATCH_CLOUD_MOCK_MODE, True),
    ):
        r = client_builds.post(
            "/api/deployments/my-service/deploy",
            json={"image_tag": "1.2.3", "environment": "dev"},
        )
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "accepted"
    assert data["mock"] is True
    assert data["service"] == "my-service"
    assert data["image_tag"] == "1.2.3"


def test_deploy_build_mock_mode_local_provider(client_builds: TestClient) -> None:
    with (
        patch(_PATCH_CLOUD_MOCK_MODE, False),
        patch(_PATCH_CLOUD_PROVIDER, "local"),
    ):
        r = client_builds.post(
            "/api/deployments/my-service/deploy",
            json={"image_tag": "0.5.1-staging", "environment": "staging"},
        )
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "accepted"
    assert data["mock"] is True


def test_deploy_build_invalid_env_returns_422(client_builds: TestClient) -> None:
    with patch(_PATCH_CLOUD_MOCK_MODE, True):
        r = client_builds.post(
            "/api/deployments/my-service/deploy",
            json={"image_tag": "1.2.3", "environment": "invalid-env"},
        )
    assert r.status_code == 422


def test_deploy_build_gcp_no_project_returns_400(client_builds: TestClient) -> None:
    with (
        patch(_PATCH_CLOUD_MOCK_MODE, False),
        patch(_PATCH_CLOUD_PROVIDER, "gcp"),
        patch(_PATCH_PROJECT_ID, ""),
    ):
        r = client_builds.post(
            "/api/deployments/my-service/deploy",
            json={"image_tag": "1.2.3", "environment": "prod"},
        )
    assert r.status_code == 400


def test_list_builds_gcp_no_project_returns_400(client_builds: TestClient) -> None:
    with (
        patch(_PATCH_CLOUD_MOCK_MODE, False),
        patch(_PATCH_CLOUD_PROVIDER, "gcp"),
        patch(_PATCH_PROJECT_ID, ""),
    ):
        r = client_builds.get("/api/builds/my-service?env=prod")
    assert r.status_code == 400


# ── list_builds AWS ECR and GCP AR live paths ─────────────────────────────────


def test_list_builds_aws_ecr_with_tags(client_builds: TestClient) -> None:
    """Lines 236-241: AWS ECR path returns sorted entries from _list_ecr_tags."""
    with (
        patch(_PATCH_CLOUD_MOCK_MODE, False),
        patch(_PATCH_CLOUD_PROVIDER, "aws"),
        patch("deployment_api.routes.builds._list_ecr_tags", return_value=["1.2.3", "0.9.0"]),
    ):
        r = client_builds.get("/api/builds/my-service?env=prod")
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 2


def test_list_builds_aws_ecr_empty_falls_back_to_manifest(client_builds: TestClient, tmp_path: Path) -> None:
    """Lines 237-239: AWS ECR empty → fallback to manifest."""
    with (
        patch(_PATCH_CLOUD_MOCK_MODE, False),
        patch(_PATCH_CLOUD_PROVIDER, "aws"),
        patch("deployment_api.routes.builds._list_ecr_tags", return_value=[]),
        patch(_PATCH_WORKSPACE_ROOT, str(tmp_path)),
    ):
        r = client_builds.get("/api/builds/my-service?env=prod")
    assert r.status_code == 200
    assert r.json() == []


def test_list_builds_gcp_ar_with_tags(client_builds: TestClient) -> None:
    """Lines 248-255: GCP AR path returns entries from _list_ar_tags."""
    with (
        patch(_PATCH_CLOUD_MOCK_MODE, False),
        patch(_PATCH_CLOUD_PROVIDER, "gcp"),
        patch(_PATCH_PROJECT_ID, "my-project"),
        patch("deployment_api.routes.builds._list_ar_tags", return_value=["1.5.0", "1.4.0"]),
    ):
        r = client_builds.get("/api/builds/execution-service?env=prod")
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 2


def test_list_builds_gcp_ar_empty_falls_back_to_manifest(client_builds: TestClient, tmp_path: Path) -> None:
    """Lines 248-252: GCP AR empty → fallback to manifest."""
    with (
        patch(_PATCH_CLOUD_MOCK_MODE, False),
        patch(_PATCH_CLOUD_PROVIDER, "gcp"),
        patch(_PATCH_PROJECT_ID, "my-project"),
        patch("deployment_api.routes.builds._list_ar_tags", return_value=[]),
        patch(_PATCH_WORKSPACE_ROOT, str(tmp_path)),
    ):
        r = client_builds.get("/api/builds/my-service?env=prod")
    assert r.status_code == 200


# ── deploy_build AWS and GCP live paths ───────────────────────────────────────


def test_deploy_build_aws_path_returns_accepted(client_builds: TestClient) -> None:
    """Lines 287-307: AWS ECS deploy path with mocked boto3.client."""
    from unittest.mock import MagicMock

    mock_sts = MagicMock()
    mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}

    with (
        patch(_PATCH_CLOUD_MOCK_MODE, False),
        patch(_PATCH_CLOUD_PROVIDER, "aws"),
        patch("boto3.client", return_value=mock_sts),
    ):
        r = client_builds.post(
            "/api/deployments/my-service/deploy",
            json={"image_tag": "1.2.3", "environment": "prod"},
        )
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "accepted"
    assert data["cloud"] == "aws"


def test_deploy_build_gcp_cloud_run_success(client_builds: TestClient) -> None:
    """Lines 314-348: GCP Cloud Run deploy via subprocess.run."""
    import subprocess
    from unittest.mock import MagicMock

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.stderr = ""

    with (
        patch(_PATCH_CLOUD_MOCK_MODE, False),
        patch(_PATCH_CLOUD_PROVIDER, "gcp"),
        patch(_PATCH_PROJECT_ID, "my-project"),
        patch("deployment_api.routes.builds.subprocess") as mock_sub,
    ):
        mock_sub.run.return_value = mock_proc
        mock_sub.TimeoutExpired = subprocess.TimeoutExpired
        r = client_builds.post(
            "/api/deployments/my-service/deploy",
            json={"image_tag": "1.2.3", "environment": "prod"},
        )
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "deploying"
    assert data["service"] == "my-service"


def test_deploy_build_gcp_cloud_run_service_name_override(client_builds: TestClient) -> None:
    """cloud_run_service_name overrides the gcloud deploy target while `service` still
    drives the AR image path — the alerting-service (image) → dp-alerting-subscriber
    (live Cloud Run service) case."""
    import subprocess
    from unittest.mock import MagicMock

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.stderr = ""

    with (
        patch(_PATCH_CLOUD_MOCK_MODE, False),
        patch(_PATCH_CLOUD_PROVIDER, "gcp"),
        patch(_PATCH_PROJECT_ID, "my-project"),
        patch("deployment_api.routes.builds.subprocess") as mock_sub,
    ):
        mock_sub.run.return_value = mock_proc
        mock_sub.TimeoutExpired = subprocess.TimeoutExpired
        r = client_builds.post(
            "/api/deployments/alerting-service/deploy",
            json={
                "image_tag": "1.2.3",
                "environment": "prod",
                "cloud_run_service_name": "dp-alerting-subscriber",
            },
        )
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "deploying"
    assert data["service"] == "alerting-service"
    assert data["cloud_run_service_name"] == "dp-alerting-subscriber"
    # The gcloud deploy target is the override, not the AR image/service name.
    deploy_args = mock_sub.run.call_args[0][0]
    assert deploy_args[0:3] == ["gcloud", "run", "deploy"]
    assert deploy_args[3] == "dp-alerting-subscriber"
    assert any("alerting-service:1.2.3" in a for a in deploy_args if a.startswith("--image="))


def test_deploy_build_gcp_cloud_run_no_override_defaults_to_service(client_builds: TestClient) -> None:
    """Omitting cloud_run_service_name preserves the existing behavior (image name ==
    Cloud Run deploy target) — no regression for every other service."""
    import subprocess
    from unittest.mock import MagicMock

    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.stderr = ""

    with (
        patch(_PATCH_CLOUD_MOCK_MODE, False),
        patch(_PATCH_CLOUD_PROVIDER, "gcp"),
        patch(_PATCH_PROJECT_ID, "my-project"),
        patch("deployment_api.routes.builds.subprocess") as mock_sub,
    ):
        mock_sub.run.return_value = mock_proc
        mock_sub.TimeoutExpired = subprocess.TimeoutExpired
        r = client_builds.post(
            "/api/deployments/my-service/deploy",
            json={"image_tag": "1.2.3", "environment": "prod"},
        )
    assert r.status_code == 200
    data = r.json()
    assert data["cloud_run_service_name"] == "my-service"
    deploy_args = mock_sub.run.call_args[0][0]
    assert deploy_args[3] == "my-service"


def test_deploy_build_gcp_cloud_run_failure_returns_502(client_builds: TestClient) -> None:
    """Lines 335-340: gcloud run deploy failure → 502."""
    import subprocess
    from unittest.mock import MagicMock

    mock_proc = MagicMock()
    mock_proc.returncode = 1
    mock_proc.stderr = "Error: revision failed to become ready"

    with (
        patch(_PATCH_CLOUD_MOCK_MODE, False),
        patch(_PATCH_CLOUD_PROVIDER, "gcp"),
        patch(_PATCH_PROJECT_ID, "my-project"),
        patch("deployment_api.routes.builds.subprocess") as mock_sub,
    ):
        mock_sub.run.return_value = mock_proc
        mock_sub.TimeoutExpired = subprocess.TimeoutExpired
        r = client_builds.post(
            "/api/deployments/my-service/deploy",
            json={"image_tag": "1.2.3", "environment": "prod"},
        )
    assert r.status_code == 502


def test_deploy_build_gcp_cloud_run_timeout_returns_504(client_builds: TestClient) -> None:
    """Line 349-350: subprocess.TimeoutExpired → 504."""
    import subprocess

    with (
        patch(_PATCH_CLOUD_MOCK_MODE, False),
        patch(_PATCH_CLOUD_PROVIDER, "gcp"),
        patch(_PATCH_PROJECT_ID, "my-project"),
        patch("deployment_api.routes.builds.subprocess") as mock_sub,
    ):
        mock_sub.run.side_effect = subprocess.TimeoutExpired(cmd="gcloud", timeout=120)
        mock_sub.TimeoutExpired = subprocess.TimeoutExpired
        r = client_builds.post(
            "/api/deployments/my-service/deploy",
            json={"image_tag": "1.2.3", "environment": "prod"},
        )
    assert r.status_code == 504


# ── _list_ecr_tags direct unit test ──────────────────────────────────────────


def test_list_ecr_tags_returns_tag_list() -> None:
    """Lines 195-208: _list_ecr_tags with mocked boto3 via sys.modules."""
    import asyncio
    import sys
    from unittest.mock import MagicMock

    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [
        {"imageIds": [{"imageTag": "1.0.0"}, {"imageTag": "0.9.0"}]},
    ]
    mock_ecr = MagicMock()
    mock_ecr.get_paginator.return_value = mock_paginator
    mock_boto3 = MagicMock()
    mock_boto3.client.return_value = mock_ecr

    sys.modules["boto3"] = mock_boto3  # type: ignore[assignment]
    try:
        # Force re-import of builds module so the deferred boto3 import picks up the mock
        import importlib

        import deployment_api.routes.builds as _builds_mod

        importlib.reload(_builds_mod)
        result = asyncio.run(_builds_mod._list_ecr_tags("my-svc"))
    finally:
        sys.modules.pop("boto3", None)
        # Reload to restore clean state
        importlib.reload(_builds_mod)

    assert "1.0.0" in result
    assert "0.9.0" in result


# ── #5 branch→image: LDR recognition + by-branch grouping ────────────────────
class TestLdrBranchRecognition:
    def test_ldr_tag_resolves_to_live_defi_rollout(self) -> None:
        from deployment_api.routes.builds import _tag_to_entry

        entry = _tag_to_entry("1.3.0-live-defi-rollout")
        assert entry.branch == "live-defi-rollout"
        assert entry.display == "1.3.0 @ live-defi-rollout"

    def test_ldr_short_slug_resolves(self) -> None:
        from deployment_api.routes.builds import _tag_to_entry

        assert _tag_to_entry("0.9.1-ldr").branch == "live-defi-rollout"


def test_list_builds_by_branch_groups_latest_and_history(client_builds: TestClient) -> None:
    """by-branch groups builds per branch with latest + full history, branches version-ordered."""
    from unittest.mock import AsyncMock

    from deployment_api.routes.builds import BuildEntry

    entries = [
        BuildEntry(tag="1.2.0", display="1.2.0 @ main", version="1.2.0", branch="main", is_v1=True),
        BuildEntry(tag="1.1.0", display="1.1.0 @ main", version="1.1.0", branch="main", is_v1=True),
        BuildEntry(
            tag="1.3.0-live-defi-rollout",
            display="1.3.0 @ live-defi-rollout",
            version="1.3.0",
            branch="live-defi-rollout",
            is_v1=True,
        ),
    ]
    with patch("deployment_api.routes.builds._resolve_build_entries", AsyncMock(return_value=entries)):
        r = client_builds.get("/api/builds/svc/by-branch?env=prod")
    assert r.status_code == 200
    body = r.json()
    by_branch = {g["branch"]: g for g in body["branches"]}
    assert set(by_branch) == {"main", "live-defi-rollout"}
    assert by_branch["main"]["latest"]["tag"] == "1.2.0"  # highest of [1.2.0, 1.1.0]
    assert len(by_branch["main"]["builds"]) == 2  # full rollback history
    # LDR (1.3.0) is the highest tip → its branch group sorts first.
    assert body["branches"][0]["branch"] == "live-defi-rollout"
