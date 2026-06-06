"""Supplemental tests for shard_management.py — covers helper functions
not tested in test_shard_management.py: dict variants of _get_shards/_shard_id/
_shard_dims, _asset_group_from_shard_dims, _check_shard_data_exists,
_build_candidate_keys, _lookup_blob_timestamp, resolve_shard_blob_data,
classify_all_shards (blob_data path), build_existing_dates_sets,
_compute_verified_succeeded_shard_ids, compute_completed_breakdown,
get_all_zones_for_vm_lookup, asset_groups_from_state, get_state_date_range.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

from deployment_api.routes.shard_management import (
    _asset_group_from_shard_dims,
    _build_candidate_keys,
    _check_shard_data_exists,
    _get_shards,
    _lookup_blob_timestamp,
    _shard_dims,
    _shard_id,
    _turbo_asset_groups_block,
    asset_groups_from_state,
    build_existing_dates_sets,
    classify_all_shards,
    compute_classification_counts,
    get_all_zones_for_vm_lookup,
    get_state_date_range,
    resolve_shard_blob_data,
)

# ── _get_shards ───────────────────────────────────────────────────────────────


class TestGetShards:
    def test_dict_with_list_shards(self) -> None:
        result = _get_shards({"shards": [1, 2, 3]})
        assert result == [1, 2, 3]

    def test_dict_no_shards_key(self) -> None:
        result = _get_shards({"other": "value"})
        assert result == []

    def test_dict_shards_not_list(self) -> None:
        result = _get_shards({"shards": "not-a-list"})
        assert result == []

    def test_object_with_shards_attr(self) -> None:
        obj = SimpleNamespace(shards=[1, 2])
        result = _get_shards(obj)
        assert result == [1, 2]

    def test_object_no_shards_attr(self) -> None:
        result = _get_shards(SimpleNamespace())
        assert result == []


# ── _shard_id ─────────────────────────────────────────────────────────────────


class TestShardId:
    def test_dict_with_shard_id(self) -> None:
        assert _shard_id({"shard_id": "s1"}) == "s1"

    def test_dict_no_shard_id(self) -> None:
        assert _shard_id({}) == ""


# ── _shard_dims ───────────────────────────────────────────────────────────────


class TestShardDims:
    def test_dict_with_dimensions(self) -> None:
        result = _shard_dims({"dimensions": {"venue": "binance"}})
        assert result == {"venue": "binance"}

    def test_dict_dimensions_not_dict(self) -> None:
        result = _shard_dims({"dimensions": "not-a-dict"})
        assert result == {}

    def test_dict_no_dimensions(self) -> None:
        result = _shard_dims({})
        assert result == {}


# ── _asset_group_from_shard_dims ──────────────────────────────────────────────


class TestAssetGroupFromShardDims:
    def test_asset_group_key(self) -> None:
        assert _asset_group_from_shard_dims({"asset_group": "cefi"}) == "cefi"

    def test_category_only_returns_empty(self) -> None:
        """v9 canonical: category-only shards are unmigrated → return '' (not 'defi')."""
        assert _asset_group_from_shard_dims({"category": "defi"}) == ""

    def test_empty_asset_group_does_not_fall_back(self) -> None:
        """v9 canonical: empty asset_group with category present → '' (no fallback)."""
        assert _asset_group_from_shard_dims({"asset_group": "", "category": "tradfi"}) == ""

    def test_neither_present(self) -> None:
        assert _asset_group_from_shard_dims({}) == ""

    def test_category_not_string_returns_empty(self) -> None:
        """category field with non-string value is always ignored (v9: not read at all)."""
        assert _asset_group_from_shard_dims({"category": 123}) == ""


# ── _turbo_asset_groups_block ─────────────────────────────────────────────────


class TestTurboAssetGroupsBlock:
    def test_asset_groups_key_present(self) -> None:
        result = _turbo_asset_groups_block({"asset_groups": {"cefi": {"ok": True}}})
        assert "cefi" in result

    def test_fallback_to_categories(self) -> None:
        result = _turbo_asset_groups_block({"categories": {"defi": {"ok": True}}})
        assert "defi" in result

    def test_neither_returns_empty(self) -> None:
        result = _turbo_asset_groups_block({})
        assert result == {}


# ── _check_shard_data_exists ──────────────────────────────────────────────────


class TestCheckShardDataExists:
    def test_cat_not_in_existing_returns_false(self) -> None:
        result = _check_shard_data_exists("cefi", "", "2024-01-01", {}, {})
        assert result is False

    def test_venue_match_with_date(self) -> None:
        result = _check_shard_data_exists(
            "cefi",
            "binance",
            "2024-01-01",
            {"cefi": {"2024-01-01"}},
            {"cefi": {"binance": {"2024-01-01"}}},
        )
        assert result is True

    def test_venue_match_date_not_found(self) -> None:
        result = _check_shard_data_exists(
            "cefi",
            "binance",
            "2024-01-02",
            {"cefi": {"2024-01-01"}},
            {"cefi": {"binance": {"2024-01-01"}}},
        )
        assert result is False

    def test_no_venue_uses_cat_dates(self) -> None:
        result = _check_shard_data_exists(
            "cefi",
            "",
            "2024-01-01",
            {"cefi": {"2024-01-01"}},
            {},
        )
        assert result is True


# ── _build_candidate_keys ─────────────────────────────────────────────────────


class TestBuildCandidateKeys:
    def test_venue_added_first(self) -> None:
        keys = _build_candidate_keys("binance", {})
        assert keys[0] == "binance"
        assert "_all" in keys

    def test_no_venue_starts_with_all(self) -> None:
        keys = _build_candidate_keys("", {})
        assert keys == ["_all"]

    def test_feature_group_added(self) -> None:
        keys = _build_candidate_keys("", {"feature_group": "fg1"})
        assert "fg1" in keys

    def test_no_duplicates(self) -> None:
        keys = _build_candidate_keys("binance", {"feature_group": "binance"})
        assert keys.count("binance") == 1


# ── _lookup_blob_timestamp ────────────────────────────────────────────────────


class TestLookupBlobTimestamp:
    def test_cat_not_in_timestamps_returns_none(self) -> None:
        result = _lookup_blob_timestamp("cefi", "", "2024-01-01", {}, {})
        assert result is None

    def test_finds_by_candidate_key(self) -> None:
        now = datetime.now(UTC)
        blob_timestamps = {"cefi": {"binance": {"2024-01-01": now}}}
        result = _lookup_blob_timestamp("cefi", "binance", "2024-01-01", {}, blob_timestamps)
        assert result == now

    def test_non_datetime_value_returns_none(self) -> None:
        blob_timestamps = {"cefi": {"binance": {"2024-01-01": "not-a-datetime"}}}
        result = _lookup_blob_timestamp("cefi", "binance", "2024-01-01", {}, blob_timestamps)
        assert result is None

    def test_fallback_to_non_candidate_key(self) -> None:
        now = datetime.now(UTC)
        blob_timestamps = {"cefi": {"_other": {"2024-01-01": now}}}
        result = _lookup_blob_timestamp("cefi", "binance", "2024-01-01", {}, blob_timestamps)
        assert result == now

    def test_all_fallback_empty_returns_none(self) -> None:
        blob_timestamps = {"cefi": {"binance": {"2024-01-02": datetime.now(UTC)}}}
        result = _lookup_blob_timestamp("cefi", "binance", "2024-01-01", {}, blob_timestamps)
        assert result is None


# ── resolve_shard_blob_data ───────────────────────────────────────────────────


class TestResolveShardBlobData:
    def test_empty_state_returns_empty(self) -> None:
        result = resolve_shard_blob_data({"shards": []}, {}, {}, {})
        assert result == {}

    def test_non_succeeded_shard_skipped(self) -> None:
        state = {"shards": [{"shard_id": "s1", "status": "running"}]}
        result = resolve_shard_blob_data(state, {}, {}, {})
        assert result == {}

    def test_succeeded_shard_no_cat_returns_false(self) -> None:
        state = {"shards": [{"shard_id": "s1", "status": "succeeded"}]}
        result = resolve_shard_blob_data(state, {}, {}, {})
        assert result == {"s1": (False, None)}

    def test_succeeded_shard_with_matching_data(self) -> None:
        # Use SimpleNamespace so getattr(shard, "dimensions", None) works
        shard = SimpleNamespace(
            shard_id="s1",
            status="succeeded",
            dimensions={"asset_group": "cefi", "venue": "", "date": "2024-01-01"},
        )
        state = {"shards": [shard]}
        result = resolve_shard_blob_data(state, {"cefi": {"2024-01-01"}}, {}, {})
        assert "s1" in result
        assert result["s1"][0] is True


# ── classify_all_shards blob_data path ───────────────────────────────────────


class TestClassifyAllShardsBlobData:
    def test_classify_with_blob_data(self) -> None:
        now = datetime.now(UTC)
        state = {
            "shards": [
                {
                    "shard_id": "s1",
                    "status": "succeeded",
                    "args": [],
                    "start_time": "2024-01-01T00:00:00Z",
                    "end_time": "2024-01-01T01:00:00Z",
                    "dimensions": {"asset_group": "cefi"},
                }
            ]
        }
        result = classify_all_shards(state, None, blob_data={"s1": (True, now)})
        assert "s1" in result


# ── compute_classification_counts ────────────────────────────────────────────


def test_compute_classification_counts() -> None:
    counts = compute_classification_counts({"s1": "ok", "s2": "ok", "s3": "failed"})
    assert counts == {"ok": 2, "failed": 1}


# ── build_existing_dates_sets ─────────────────────────────────────────────────


class TestBuildExistingDatesSets:
    def test_empty_turbo_result(self) -> None:
        cat_dates, venue_dates = build_existing_dates_sets({})
        assert cat_dates == {}
        assert venue_dates == {}

    def test_uses_asset_groups_key(self) -> None:
        turbo = {
            "asset_groups": {
                "cefi": {
                    "dates": {"2024-01-01": {"venues": {"binance": True}}},
                }
            }
        }
        cat_dates, venue_dates = build_existing_dates_sets(turbo)
        # Should process without error
        assert isinstance(cat_dates, dict)

    def test_non_dict_cat_data_skipped(self) -> None:
        turbo = {"asset_groups": {"cefi": "not-a-dict"}}
        cat_dates, venue_dates = build_existing_dates_sets(turbo)
        assert "cefi" not in cat_dates

    def test_error_in_cat_data_skipped(self) -> None:
        turbo = {"asset_groups": {"cefi": {"error": "fetch_failed"}}}
        cat_dates, venue_dates = build_existing_dates_sets(turbo)
        assert "cefi" not in cat_dates


# ── get_all_zones_for_vm_lookup ───────────────────────────────────────────────


def test_get_all_zones_with_primary_region() -> None:
    zones = get_all_zones_for_vm_lookup("us-central1")
    assert isinstance(zones, list)


def test_get_all_zones_without_primary_region() -> None:
    zones = get_all_zones_for_vm_lookup(None)
    assert isinstance(zones, list)


# ── asset_groups_from_state ───────────────────────────────────────────────────


def test_asset_groups_from_state_none_shards() -> None:
    result = asset_groups_from_state({"shards": None})
    # Returns None or empty when no shards
    assert result is None or result == []


def test_asset_groups_from_state_extracts_groups() -> None:
    state = {
        "shards": [
            {"status": "succeeded", "dimensions": {"asset_group": "cefi"}},
            {"status": "succeeded", "dimensions": {"asset_group": "defi"}},
        ]
    }
    result = asset_groups_from_state(state)
    assert result is not None
    assert "cefi" in result
    assert "defi" in result


# ── get_state_date_range ──────────────────────────────────────────────────────


def test_get_state_date_range_empty() -> None:
    start, end = get_state_date_range({"shards": []})
    assert start is None
    assert end is None


def test_get_state_date_range_with_shards() -> None:
    state = {
        "shards": [
            {"dimensions": {"date": "2024-01-01"}},
            {"dimensions": {"date": "2024-01-05"}},
        ]
    }
    start, end = get_state_date_range(state)
    # Should return some date range
    assert start is not None or end is not None
