"""
Unit tests for checklist module pure helper functions.

Tests cover:
- _get_checklist_path
- _format_phase_name
- _parse_checklist
- _get_warnings
- _load_checklist_yaml
"""

import tempfile
from pathlib import Path

import pytest
import yaml

from deployment_api.routes.checklist import (
    _format_phase_name,
    _get_checklist_path,
    _get_warnings,
    _load_checklist_yaml,
    _parse_checklist,
)


class TestGetChecklistPath:
    """Tests for _get_checklist_path."""

    def test_returns_correct_path(self):
        config_dir = Path("/tmp/configs")
        result = _get_checklist_path(config_dir, "instruments-service")
        assert result == Path("/tmp/configs/checklist.instruments-service.yaml")

    def test_different_service_names(self):
        config_dir = Path("/app/configs")
        result = _get_checklist_path(config_dir, "market-data")
        assert result == Path("/app/configs/checklist.market-data.yaml")


class TestFormatPhaseName:
    """Tests for _format_phase_name."""

    def test_removes_phase_prefix(self):
        result = _format_phase_name("phase_1_repository_foundation")
        assert result == "Repository Foundation"

    def test_handles_single_word(self):
        result = _format_phase_name("phase_2_testing")
        assert result == "Testing"

    def test_handles_multi_word(self):
        result = _format_phase_name("phase_3_cloud_infrastructure_setup")
        assert result == "Cloud Infrastructure Setup"

    def test_no_phase_prefix(self):
        # String without phase_ prefix passes through unchanged
        result = _format_phase_name("no_prefix_here")
        assert result == "No Prefix Here"

    def test_empty_string(self):
        result = _format_phase_name("")
        assert result == ""

    def test_single_phase_digit(self):
        result = _format_phase_name("phase_9_final")
        assert result == "Final"


class TestParseChecklist:
    """Tests for _parse_checklist."""

    def _make_checklist_data(self, phases=None, service="test-service", last_updated="2026-01-01"):
        data: dict[str, object] = {"service_name": service, "last_updated": last_updated}
        if phases:
            data.update(phases)
        return data

    def test_empty_checklist(self):
        data = self._make_checklist_data()
        result = _parse_checklist(data)
        assert result["service"] == "test-service"
        assert result["total_items"] == 0
        assert result["completed_items"] == 0
        assert result["readiness_percent"] == 100
        assert result["categories"] == []
        assert result["blocking_items"] == []

    def test_all_done_items(self):
        data = self._make_checklist_data(
            phases={
                "phase_1_setup": {
                    "item_a": {"status": "done", "description": "Item A"},
                    "item_b": {"status": "done", "description": "Item B"},
                }
            }
        )
        result = _parse_checklist(data)
        assert result["total_items"] == 2
        assert result["completed_items"] == 2
        assert result["readiness_percent"] == 100

    def test_pending_items(self):
        data = self._make_checklist_data(
            phases={
                "phase_1_setup": {
                    "item_a": {"status": "done", "description": "Item A"},
                    "item_b": {"status": "pending", "description": "Item B"},
                }
            }
        )
        result = _parse_checklist(data)
        assert result["total_items"] == 2
        assert result["pending_items"] == 1
        assert result["readiness_percent"] == 50

    def test_partial_items_count_as_half(self):
        data = self._make_checklist_data(
            phases={
                "phase_1_setup": {
                    "item_a": {"status": "partial", "description": "Item A"},
                    "item_b": {"status": "partial", "description": "Item B"},
                }
            }
        )
        result = _parse_checklist(data)
        assert result["partial_items"] == 2
        # 2 partial = 1 effective done, 2 total applicable => 50%
        assert result["readiness_percent"] == 50

    def test_na_items_excluded_from_percent(self):
        data = self._make_checklist_data(
            phases={
                "phase_1_setup": {
                    "item_a": {"status": "done", "description": "Item A"},
                    "item_b": {"status": "n/a", "description": "Item B"},
                }
            }
        )
        result = _parse_checklist(data)
        assert result["not_applicable_items"] == 1
        # Only 1 applicable item, 1 done -> 100%
        assert result["readiness_percent"] == 100

    def test_blocking_items_tracked(self):
        data = self._make_checklist_data(
            phases={
                "phase_1_foundation": {
                    "item_a": {"status": "pending", "description": "Critical item", "blocking": True},
                    "item_b": {"status": "done", "description": "Done item", "blocking": True},
                }
            }
        )
        result = _parse_checklist(data)
        # Only pending blocking items appear in blocking_items
        assert len(result["blocking_items"]) == 1
        assert result["blocking_items"][0]["id"] == "item_a"

    def test_not_started_is_blocking(self):
        data = self._make_checklist_data(
            phases={
                "phase_1_foundation": {
                    "item_x": {"status": "not_started", "description": "Not started item", "blocking": True},
                }
            }
        )
        result = _parse_checklist(data)
        assert len(result["blocking_items"]) == 1

    def test_multiple_phases(self):
        data = self._make_checklist_data(
            phases={
                "phase_1_foundation": {
                    "item_a": {"status": "done", "description": "Done"},
                },
                "phase_2_testing": {
                    "item_b": {"status": "pending", "description": "Pending"},
                },
            }
        )
        result = _parse_checklist(data)
        assert result["total_items"] == 2
        assert len(result["categories"]) == 2

    def test_phase_sort_order(self):
        data = self._make_checklist_data(
            phases={
                "phase_3_deploy": {
                    "item_a": {"status": "done", "description": "Item"},
                },
                "phase_1_setup": {
                    "item_b": {"status": "done", "description": "Item"},
                },
            }
        )
        result = _parse_checklist(data)
        # Phases are sorted, so phase_1 comes first
        assert result["categories"][0]["name"] == "phase_1_setup"

    def test_category_structure(self):
        data = self._make_checklist_data(
            phases={
                "phase_1_repository_foundation": {
                    "item_a": {"status": "done", "description": "A done item"},
                }
            }
        )
        result = _parse_checklist(data)
        cat = result["categories"][0]
        assert cat["name"] == "phase_1_repository_foundation"
        assert cat["display_name"] == "Repository Foundation"
        assert cat["percent"] == 100
        assert cat["total_items"] == 1
        assert len(cat["items"]) == 1

    def test_last_updated_passed_through(self):
        data = self._make_checklist_data(last_updated="2026-03-04")
        result = _parse_checklist(data)
        assert result["last_updated"] == "2026-03-04"

    def test_unknown_service_defaults(self):
        # No service_name key -> defaults to "unknown"
        data: dict[str, object] = {}
        result = _parse_checklist(data)
        assert result["service"] == "unknown"

    def test_non_dict_phase_data_skipped(self):
        data: dict[str, object] = {
            "service_name": "svc",
            "phase_1_setup": "not_a_dict",
        }
        result = _parse_checklist(data)
        # Non-dict phases are silently skipped
        assert result["total_items"] == 0

    def test_blocking_items_include_category_display_name(self):
        data = self._make_checklist_data(
            phases={
                "phase_2_integration": {
                    "item_x": {"status": "pending", "description": "Blocker", "blocking": True},
                }
            }
        )
        result = _parse_checklist(data)
        assert result["blocking_items"][0]["category"] == "Integration"


class TestGetWarnings:
    """Tests for _get_warnings."""

    def _make_checklist(self, items):
        """Build a parsed checklist dict with given items."""
        return {
            "categories": [
                {
                    "items": [
                        {
                            "id": k,
                            "description": v["description"],
                            "status": v["status"],
                            "blocking": v.get("blocking", False),
                        }
                        for k, v in items.items()
                    ]
                }
            ]
        }

    def test_pending_non_blocking_is_warning(self):
        checklist = self._make_checklist(
            {
                "item_a": {"status": "pending", "description": "Do this", "blocking": False},
            }
        )
        warnings = _get_warnings(checklist)
        assert len(warnings) == 1
        assert "Do this" in warnings[0]

    def test_partial_non_blocking_is_warning(self):
        checklist = self._make_checklist(
            {
                "item_a": {"status": "partial", "description": "Almost done", "blocking": False},
            }
        )
        warnings = _get_warnings(checklist)
        assert len(warnings) == 1

    def test_done_items_not_in_warnings(self):
        checklist = self._make_checklist(
            {
                "item_a": {"status": "done", "description": "Done", "blocking": False},
            }
        )
        warnings = _get_warnings(checklist)
        assert warnings == []

    def test_blocking_items_excluded_from_warnings(self):
        checklist = self._make_checklist(
            {
                "item_a": {"status": "pending", "description": "Blocking item", "blocking": True},
            }
        )
        warnings = _get_warnings(checklist)
        assert warnings == []

    def test_warnings_capped_at_10(self):
        items = {f"item_{i}": {"status": "pending", "description": f"Item {i}", "blocking": False} for i in range(15)}
        checklist = self._make_checklist(items)
        warnings = _get_warnings(checklist)
        assert len(warnings) == 10

    def test_empty_checklist(self):
        checklist: dict[str, object] = {"categories": []}
        warnings = _get_warnings(checklist)
        assert warnings == []


class TestLoadChecklistYaml:
    """Tests for _load_checklist_yaml."""

    def test_loads_valid_yaml(self):
        content = {
            "service_name": "my-service",
            "last_updated": "2026-01-01",
            "phase_1_setup": {"item_a": {"status": "done", "description": "A"}},
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            config_dir = Path(tmpdir)
            checklist_path = config_dir / "checklist.my-service.yaml"
            with open(checklist_path, "w") as f:
                yaml.dump(content, f)

            result = _load_checklist_yaml(config_dir, "my-service")
            assert result["service_name"] == "my-service"

    def test_raises_file_not_found_for_missing_service(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_dir = Path(tmpdir)
            with pytest.raises(FileNotFoundError):
                _load_checklist_yaml(config_dir, "nonexistent-service")
