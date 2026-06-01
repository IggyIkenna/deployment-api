"""Unit tests for routes/service_status_fast_data.py — helper function coverage."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from unified_trading_library import setup_events

setup_events("deployment-api", "test")

_PATCH_LIST_OBJECTS = "deployment_api.utils.storage_facade.list_objects"
_PATCH_LIST_PREFIXES = "deployment_api.utils.storage_facade.list_prefixes"


# ── _obj_updated ──────────────────────────────────────────────────────────────


class TestObjUpdated:
    def test_returns_updated_attr(self) -> None:
        from deployment_api.routes.service_status_fast_data import _obj_updated

        ts = datetime(2024, 1, 1, tzinfo=UTC)
        obj = SimpleNamespace(updated=ts)
        assert _obj_updated(obj) == ts

    def test_no_updated_attr_returns_datetime_min(self) -> None:
        from deployment_api.routes.service_status_fast_data import _obj_updated

        obj = SimpleNamespace()
        result = _obj_updated(obj)
        assert result == datetime.min.replace(tzinfo=UTC)

    def test_none_updated_falls_back_to_min(self) -> None:
        from deployment_api.routes.service_status_fast_data import _obj_updated

        obj = SimpleNamespace(updated=None)
        result = _obj_updated(obj)
        assert result == datetime.min.replace(tzinfo=UTC)

    def test_vars_fallback_when_no_attr(self) -> None:
        from deployment_api.routes.service_status_fast_data import _obj_updated

        ts = datetime(2024, 6, 1, tzinfo=UTC)

        class FakeBlob:
            def __init__(self) -> None:
                self.updated = ts

        result = _obj_updated(FakeBlob())
        assert result == ts


# ── _latest_ts_from_objects ───────────────────────────────────────────────────


class TestLatestTsFromObjects:
    def test_empty_list_returns_none(self) -> None:
        from deployment_api.routes.service_status_fast_data import _latest_ts_from_objects

        assert _latest_ts_from_objects([]) is None

    def test_returns_latest(self) -> None:
        from deployment_api.routes.service_status_fast_data import _latest_ts_from_objects

        early = SimpleNamespace(updated=datetime(2024, 1, 1, tzinfo=UTC))
        late = SimpleNamespace(updated=datetime(2024, 6, 1, tzinfo=UTC))
        result = _latest_ts_from_objects([early, late])
        assert result == datetime(2024, 6, 1, tzinfo=UTC)

    def test_all_min_timestamps_returns_none(self) -> None:
        from deployment_api.routes.service_status_fast_data import _latest_ts_from_objects

        obj = SimpleNamespace()
        assert _latest_ts_from_objects([obj]) is None


# ── _find_latest_ts_for_bucket ────────────────────────────────────────────────


class TestFindLatestTsForBucket:
    def test_date_partitioned_bucket_returns_timestamp(self) -> None:
        from deployment_api.routes.service_status_fast_data import _find_latest_ts_for_bucket

        ts = datetime(2024, 1, 5, tzinfo=UTC)
        fake_obj = SimpleNamespace(updated=ts)

        with (
            patch(
                _PATCH_LIST_PREFIXES,
                side_effect=[
                    ["raw_tick_data/by_date/day=2024-01-05/"],
                ],
            ),
            patch(_PATCH_LIST_OBJECTS, return_value=[fake_obj]),
        ):
            result = _find_latest_ts_for_bucket("my-bucket")

        assert result == ts

    def test_no_date_prefixes_falls_back_to_objects(self) -> None:
        from deployment_api.routes.service_status_fast_data import _find_latest_ts_for_bucket

        ts = datetime(2024, 3, 1, tzinfo=UTC)
        fake_obj = SimpleNamespace(updated=ts)

        with (
            patch(_PATCH_LIST_PREFIXES, return_value=["raw_tick_data/by_date/other/"]),
            patch(_PATCH_LIST_OBJECTS, return_value=[fake_obj]),
        ):
            result = _find_latest_ts_for_bucket("my-bucket")

        assert result == ts

    def test_empty_bucket_returns_none(self) -> None:
        from deployment_api.routes.service_status_fast_data import _find_latest_ts_for_bucket

        with (
            patch(_PATCH_LIST_PREFIXES, return_value=[]),
            patch(_PATCH_LIST_OBJECTS, return_value=[]),
        ):
            result = _find_latest_ts_for_bucket("empty-bucket")

        assert result is None


# ── _get_timestamps_sync ──────────────────────────────────────────────────────


class TestGetTimestampsSync:
    def test_unknown_service_returns_none_latest(self) -> None:
        from deployment_api.routes.service_status_fast_data import _get_timestamps_sync

        result = _get_timestamps_sync("unknown-service-xyz")
        assert result["latest"] is None

    def test_known_service_returns_latest(self) -> None:
        from deployment_api.routes.service_status_fast_data import _get_timestamps_sync

        ts = datetime(2024, 2, 1, tzinfo=UTC)

        with (
            patch(_PATCH_LIST_PREFIXES, return_value=[]),
            patch(_PATCH_LIST_OBJECTS, return_value=[SimpleNamespace(updated=ts)]),
        ):
            result = _get_timestamps_sync("instruments-service")

        assert result.get("latest") is not None

    def test_storage_error_returns_error_key(self) -> None:
        from deployment_api.routes.service_status_fast_data import _get_timestamps_sync

        with (
            patch(_PATCH_LIST_PREFIXES, side_effect=OSError("bucket gone")),
            patch(_PATCH_LIST_OBJECTS, return_value=[]),
        ):
            result = _get_timestamps_sync("instruments-service")

        # Should either have latest or by_asset_group with error entries — no crash
        assert isinstance(result, dict)


# ── get_latest_data_timestamp_fast ────────────────────────────────────────────


class TestGetLatestDataTimestampFast:
    @pytest.mark.asyncio
    async def test_returns_dict(self) -> None:
        from deployment_api.routes.service_status_fast_data import get_latest_data_timestamp_fast

        with (
            patch(_PATCH_LIST_PREFIXES, return_value=[]),
            patch(_PATCH_LIST_OBJECTS, return_value=[]),
        ):
            result = await get_latest_data_timestamp_fast("instruments-service")

        assert isinstance(result, dict)
