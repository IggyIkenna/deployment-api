"""Unit tests for deployment_api/clients/deployment_service_client.py.

All aiohttp I/O is mocked via patch(_make_session). Tests cover:
  - Happy path for all 12 async functions
  - Optional payload field branches (start_date, end_date, tag, vm_zone, shard_id, etc.)
  - Non-200 error paths that raise RuntimeError
  - Edge cases (empty statuses dict, non-list events, etc.)
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

_PATCH_MAKE_SESSION = "deployment_api.clients.deployment_service_client._make_session"


def _mock_session(status: int = 200, json_data: object = None, text_data: str = "bad request") -> MagicMock:
    """Build a fully-mocked aiohttp session + response pair."""
    resp = MagicMock()
    resp.status = status
    resp.__aenter__ = AsyncMock(return_value=resp)
    resp.__aexit__ = AsyncMock(return_value=False)
    resp.json = AsyncMock(return_value=json_data if json_data is not None else {})
    resp.text = AsyncMock(return_value=text_data)

    session = MagicMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    session.post = MagicMock(return_value=resp)
    session.get = MagicMock(return_value=resp)
    return session


# ── calculate_shards ──────────────────────────────────────────────────────────


class TestCalculateShards:
    def test_returns_shards_list_on_200(self) -> None:
        from deployment_api.clients.deployment_service_client import calculate_shards

        shards_data = [{"shard_index": 0, "total_shards": 2}, {"shard_index": 1, "total_shards": 2}]
        session = _mock_session(200, {"shards": shards_data})

        with patch(_PATCH_MAKE_SESSION, return_value=session):
            result = asyncio.run(calculate_shards("execution-service", "/config"))
        assert result == shards_data

    def test_with_all_optional_params(self) -> None:
        from deployment_api.clients.deployment_service_client import calculate_shards

        session = _mock_session(200, {"shards": []})

        with patch(_PATCH_MAKE_SESSION, return_value=session):
            result = asyncio.run(
                calculate_shards(
                    "strategy-service",
                    "/config",
                    start_date="2026-01-01",
                    end_date="2026-05-01",
                    cloud_config_path="gs://bucket/config",
                    date_granularity_override="weekly",
                    max_shards=50,
                )
            )
        assert result == []
        call_kwargs = session.post.call_args
        payload = call_kwargs[1]["json"]
        assert payload["start_date"] == "2026-01-01"
        assert payload["end_date"] == "2026-05-01"
        assert payload["cloud_config_path"] == "gs://bucket/config"
        assert payload["date_granularity_override"] == "weekly"

    def test_non_200_raises_runtime_error(self) -> None:
        from deployment_api.clients.deployment_service_client import calculate_shards

        session = _mock_session(500, text_data="Internal Server Error")

        with patch(_PATCH_MAKE_SESSION, return_value=session):
            with pytest.raises(RuntimeError, match="HTTP 500"):
                asyncio.run(calculate_shards("svc", "/config"))

    def test_non_list_shards_returns_empty(self) -> None:
        from deployment_api.clients.deployment_service_client import calculate_shards

        session = _mock_session(200, {"shards": None})

        with patch(_PATCH_MAKE_SESSION, return_value=session):
            result = asyncio.run(calculate_shards("svc", "/config"))
        assert result == []


# ── create_deployment ─────────────────────────────────────────────────────────


class TestCreateDeployment:
    def test_returns_result_on_200(self) -> None:
        from deployment_api.clients.deployment_service_client import create_deployment

        resp_data = {"deployment_id": "dep-xyz", "status": "accepted"}
        session = _mock_session(200, resp_data)

        with patch(_PATCH_MAKE_SESSION, return_value=session):
            result = asyncio.run(
                create_deployment(
                    deployment_id="dep-xyz",
                    service="execution-service",
                    region="us-central1",
                    compute_type="cloud_run",
                    deployment_mode="batch",
                    docker_image="gcr.io/proj/img:latest",
                    job_name="exec-job",
                    compute_config={"cpu": 2},
                    env_vars={"FOO": "bar"},
                    max_concurrent=4,
                    shards=[],
                )
            )
        assert result["deployment_id"] == "dep-xyz"

    def test_with_tag_and_vm_zone(self) -> None:
        from deployment_api.clients.deployment_service_client import create_deployment

        session = _mock_session(201, {"deployment_id": "dep-1", "status": "ok"})

        with patch(_PATCH_MAKE_SESSION, return_value=session):
            _ = asyncio.run(
                create_deployment(
                    deployment_id="dep-1",
                    service="svc",
                    region="us-east1",
                    compute_type="vm",
                    deployment_mode="live",
                    docker_image="gcr.io/proj/img:v2",
                    job_name="vm-job",
                    compute_config={},
                    env_vars={},
                    max_concurrent=1,
                    shards=[],
                    tag="v2.0.0",
                    vm_zone="us-east1-b",
                )
            )
        payload = session.post.call_args[1]["json"]
        assert payload["tag"] == "v2.0.0"
        assert payload["vm_zone"] == "us-east1-b"

    def test_non_200_raises_runtime_error(self) -> None:
        from deployment_api.clients.deployment_service_client import create_deployment

        session = _mock_session(409, text_data="Conflict")

        with patch(_PATCH_MAKE_SESSION, return_value=session):
            with pytest.raises(RuntimeError, match="HTTP 409"):
                asyncio.run(
                    create_deployment(
                        deployment_id="d",
                        service="s",
                        region="r",
                        compute_type="c",
                        deployment_mode="m",
                        docker_image="img",
                        job_name="j",
                        compute_config={},
                        env_vars={},
                        max_concurrent=1,
                        shards=[],
                    )
                )


# ── get_data_status ───────────────────────────────────────────────────────────


class TestGetDataStatus:
    def test_returns_dict_on_200(self) -> None:
        from deployment_api.clients.deployment_service_client import get_data_status

        resp_data = {"coverage": 0.95}
        session = _mock_session(200, resp_data)

        with patch(_PATCH_MAKE_SESSION, return_value=session):
            result = asyncio.run(get_data_status("strategy-service", "2026-01-01", "2026-05-01"))
        assert result["coverage"] == 0.95

    def test_with_asset_groups_and_venues(self) -> None:
        from deployment_api.clients.deployment_service_client import get_data_status

        session = _mock_session(200, {})

        with patch(_PATCH_MAKE_SESSION, return_value=session):
            asyncio.run(
                get_data_status(
                    "svc",
                    "2026-01-01",
                    "2026-05-01",
                    asset_groups=["defi"],
                    venues=["uniswap"],
                )
            )
        params = session.get.call_args[1]["params"]
        assert params["asset_groups"] == ["defi"]
        assert params["venues"] == ["uniswap"]

    def test_non_200_raises_runtime_error(self) -> None:
        from deployment_api.clients.deployment_service_client import get_data_status

        session = _mock_session(503, text_data="Service Unavailable")

        with patch(_PATCH_MAKE_SESSION, return_value=session):
            with pytest.raises(RuntimeError, match="HTTP 503"):
                asyncio.run(get_data_status("svc", "2026-01-01", "2026-05-01"))


# ── cancel_vm_jobs ────────────────────────────────────────────────────────────


class TestCancelVmJobs:
    def test_returns_result_on_200(self) -> None:
        from deployment_api.clients.deployment_service_client import cancel_vm_jobs

        resp_data = {"cancelled_count": 3, "failed_count": 0}
        session = _mock_session(200, resp_data)

        with patch(_PATCH_MAKE_SESSION, return_value=session):
            result = asyncio.run(
                cancel_vm_jobs(
                    deployment_id="dep-1",
                    project_id="my-proj",
                    region="us-central1",
                    service_account_email="sa@proj.iam.gserviceaccount.com",
                    state_bucket="state-bucket",
                    state_prefix="prefix",
                    job_name="vm-job",
                    jobs=[("job-1", "us-central1-a"), ("job-2", None)],
                )
            )
        assert result["cancelled_count"] == 3

    def test_non_200_raises_runtime_error(self) -> None:
        from deployment_api.clients.deployment_service_client import cancel_vm_jobs

        session = _mock_session(500, text_data="server error")

        with patch(_PATCH_MAKE_SESSION, return_value=session):
            with pytest.raises(RuntimeError, match="HTTP 500"):
                asyncio.run(cancel_vm_jobs("d", "p", "r", "sa@p.com", "bucket", "prefix", "job", [("j1", None)]))


# ── get_vm_status_batch ───────────────────────────────────────────────────────


class TestGetVmStatusBatch:
    def test_returns_statuses_dict(self) -> None:
        from deployment_api.clients.deployment_service_client import get_vm_status_batch

        resp_data = {"statuses": {"vm-1": "RUNNING", "vm-2": "TERMINATED"}}
        session = _mock_session(200, resp_data)

        with patch(_PATCH_MAKE_SESSION, return_value=session):
            result = asyncio.run(get_vm_status_batch("my-proj", "us-central1-a", ["vm-1", "vm-2"]))
        assert result == {"vm-1": "RUNNING", "vm-2": "TERMINATED"}

    def test_non_dict_statuses_returns_empty(self) -> None:
        from deployment_api.clients.deployment_service_client import get_vm_status_batch

        session = _mock_session(200, {"statuses": None})

        with patch(_PATCH_MAKE_SESSION, return_value=session):
            result = asyncio.run(get_vm_status_batch("proj", "zone", ["vm-1"]))
        assert result == {}

    def test_non_200_raises_runtime_error(self) -> None:
        from deployment_api.clients.deployment_service_client import get_vm_status_batch

        session = _mock_session(404, text_data="Not Found")

        with patch(_PATCH_MAKE_SESSION, return_value=session):
            with pytest.raises(RuntimeError, match="HTTP 404"):
                asyncio.run(get_vm_status_batch("proj", "zone", []))


# ── quota_acquire_batch ───────────────────────────────────────────────────────


class TestQuotaAcquireBatch:
    def test_returns_acquired_count(self) -> None:
        from deployment_api.clients.deployment_service_client import quota_acquire_batch

        session = _mock_session(200, {"acquired": 5})

        with patch(_PATCH_MAKE_SESSION, return_value=session):
            result = asyncio.run(quota_acquire_batch({"resource": "vm"}, 10))
        assert result == 5

    def test_non_numeric_acquired_returns_zero(self) -> None:
        from deployment_api.clients.deployment_service_client import quota_acquire_batch

        session = _mock_session(200, {"acquired": "invalid"})

        with patch(_PATCH_MAKE_SESSION, return_value=session):
            result = asyncio.run(quota_acquire_batch({}, 5))
        assert result == 0

    def test_non_200_raises_runtime_error(self) -> None:
        from deployment_api.clients.deployment_service_client import quota_acquire_batch

        session = _mock_session(429, text_data="Too Many Requests")

        with patch(_PATCH_MAKE_SESSION, return_value=session):
            with pytest.raises(RuntimeError, match="HTTP 429"):
                asyncio.run(quota_acquire_batch({}, 5))


# ── get_cloud_run_status_batch ────────────────────────────────────────────────


class TestGetCloudRunStatusBatch:
    def test_returns_statuses_dict(self) -> None:
        from deployment_api.clients.deployment_service_client import get_cloud_run_status_batch

        resp_data = {"statuses": {"job-1": "SUCCEEDED", "job-2": "FAILED"}}
        session = _mock_session(200, resp_data)

        with patch(_PATCH_MAKE_SESSION, return_value=session):
            result = asyncio.run(
                get_cloud_run_status_batch(
                    "proj", "us-central1", "sa@proj.iam.gserviceaccount.com", "job", ["job-1", "job-2"]
                )
            )
        assert result == {"job-1": "SUCCEEDED", "job-2": "FAILED"}

    def test_non_200_raises_runtime_error(self) -> None:
        from deployment_api.clients.deployment_service_client import get_cloud_run_status_batch

        session = _mock_session(500, text_data="error")

        with patch(_PATCH_MAKE_SESSION, return_value=session):
            with pytest.raises(RuntimeError, match="HTTP 500"):
                asyncio.run(get_cloud_run_status_batch("p", "r", "sa", "j", []))


# ── quota_release_batch ───────────────────────────────────────────────────────


class TestQuotaReleaseBatch:
    def test_succeeds_on_200(self) -> None:
        from deployment_api.clients.deployment_service_client import quota_release_batch

        session = _mock_session(200, {})

        with patch(_PATCH_MAKE_SESSION, return_value=session):
            asyncio.run(quota_release_batch({"resource": "vm"}, 5))  # no exception

    def test_succeeds_on_204(self) -> None:
        from deployment_api.clients.deployment_service_client import quota_release_batch

        session = _mock_session(204, {})

        with patch(_PATCH_MAKE_SESSION, return_value=session):
            asyncio.run(quota_release_batch({}, 3))  # no exception

    def test_non_200_raises_runtime_error(self) -> None:
        from deployment_api.clients.deployment_service_client import quota_release_batch

        session = _mock_session(500, text_data="server error")

        with patch(_PATCH_MAKE_SESSION, return_value=session):
            with pytest.raises(RuntimeError, match="HTTP 500"):
                asyncio.run(quota_release_batch({}, 1))


# ── get_deployment_events ─────────────────────────────────────────────────────


class TestGetDeploymentEvents:
    def test_returns_events_list(self) -> None:
        from deployment_api.clients.deployment_service_client import get_deployment_events

        events = [{"event_type": "SHARD_STARTED", "shard_id": "s-1"}]
        session = _mock_session(200, {"events": events})

        with patch(_PATCH_MAKE_SESSION, return_value=session):
            result = asyncio.run(get_deployment_events("dep-123"))
        assert result == events

    def test_with_shard_id_passes_param(self) -> None:
        from deployment_api.clients.deployment_service_client import get_deployment_events

        session = _mock_session(200, {"events": []})

        with patch(_PATCH_MAKE_SESSION, return_value=session):
            result = asyncio.run(get_deployment_events("dep-123", shard_id="shard-7"))
        assert result == []
        params = session.get.call_args[1]["params"]
        assert params["shard_id"] == "shard-7"

    def test_non_list_events_returns_empty(self) -> None:
        from deployment_api.clients.deployment_service_client import get_deployment_events

        session = _mock_session(200, {"events": None})

        with patch(_PATCH_MAKE_SESSION, return_value=session):
            result = asyncio.run(get_deployment_events("dep-123"))
        assert result == []

    def test_non_200_raises_runtime_error(self) -> None:
        from deployment_api.clients.deployment_service_client import get_deployment_events

        session = _mock_session(404, text_data="Not Found")

        with patch(_PATCH_MAKE_SESSION, return_value=session):
            with pytest.raises(RuntimeError, match="HTTP 404"):
                asyncio.run(get_deployment_events("dep-404"))


# ── get_vm_events ─────────────────────────────────────────────────────────────


class TestGetVmEvents:
    def test_returns_events_list(self) -> None:
        from deployment_api.clients.deployment_service_client import get_vm_events

        events = [{"event_type": "VM_PREEMPTED"}]
        session = _mock_session(200, {"events": events})

        with patch(_PATCH_MAKE_SESSION, return_value=session):
            result = asyncio.run(get_vm_events("dep-abc"))
        assert result == events

    def test_non_200_raises_runtime_error(self) -> None:
        from deployment_api.clients.deployment_service_client import get_vm_events

        session = _mock_session(500, text_data="error")

        with patch(_PATCH_MAKE_SESSION, return_value=session):
            with pytest.raises(RuntimeError, match="HTTP 500"):
                asyncio.run(get_vm_events("dep-abc"))


# ── live_rollback ─────────────────────────────────────────────────────────────


class TestLiveRollback:
    def test_returns_result_on_200(self) -> None:
        from deployment_api.clients.deployment_service_client import live_rollback

        resp_data = {"deployment_id": "dep-live", "status": "rolling_back"}
        session = _mock_session(200, resp_data)

        with patch(_PATCH_MAKE_SESSION, return_value=session):
            result = asyncio.run(live_rollback("dep-live", "exec-svc", "us-central1"))
        assert result["status"] == "rolling_back"

    def test_with_target_revision(self) -> None:
        from deployment_api.clients.deployment_service_client import live_rollback

        session = _mock_session(202, {"deployment_id": "dep-live", "status": "accepted"})

        with patch(_PATCH_MAKE_SESSION, return_value=session):
            _ = asyncio.run(live_rollback("dep-live", "svc", "region", target_revision="rev-001"))
        payload = session.post.call_args[1]["json"]
        assert payload["target_revision"] == "rev-001"

    def test_non_200_raises_runtime_error(self) -> None:
        from deployment_api.clients.deployment_service_client import live_rollback

        session = _mock_session(422, text_data="Unprocessable Entity")

        with patch(_PATCH_MAKE_SESSION, return_value=session):
            with pytest.raises(RuntimeError, match="HTTP 422"):
                asyncio.run(live_rollback("dep", "svc", "r"))


# ── get_live_health ───────────────────────────────────────────────────────────


class TestGetLiveHealth:
    def test_returns_health_dict_on_200(self) -> None:
        from deployment_api.clients.deployment_service_client import get_live_health

        resp_data = {"deployment_id": "dep-1", "healthy": True, "status_code": 200}
        session = _mock_session(200, resp_data)

        with patch(_PATCH_MAKE_SESSION, return_value=session):
            result = asyncio.run(get_live_health("dep-1", "exec-svc", "us-central1"))
        assert result["healthy"] is True

    def test_non_200_raises_runtime_error(self) -> None:
        from deployment_api.clients.deployment_service_client import get_live_health

        session = _mock_session(503, text_data="Service Unavailable")

        with patch(_PATCH_MAKE_SESSION, return_value=session):
            with pytest.raises(RuntimeError, match="HTTP 503"):
                asyncio.run(get_live_health("dep-1", "svc", "r"))


# ── _make_session and settings integration ────────────────────────────────────


class TestMakeSessionIntegration:
    def test_calculate_shards_uses_base_url_from_settings(self) -> None:
        """Verify _base_url is called to construct the URL (covers _base_url + _timeout)."""
        from deployment_api.clients.deployment_service_client import calculate_shards

        session = _mock_session(200, {"shards": []})

        with (
            patch(_PATCH_MAKE_SESSION, return_value=session),
            patch("deployment_api.clients.deployment_service_client._settings") as mock_settings,
        ):
            mock_settings.DEPLOYMENT_SERVICE_URL = "http://ds-svc:9000"
            mock_settings.DEPLOYMENT_SERVICE_TIMEOUT_SECONDS = 30
            result = asyncio.run(calculate_shards("svc", "/cfg"))
        assert result == []
        url_called = session.post.call_args[0][0]
        assert url_called == "http://ds-svc:9000/api/v1/shards/calculate"
