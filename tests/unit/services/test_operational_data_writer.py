"""Unit tests for operational_data_writer.py — best-effort BQ writes for idle_spend/reap_events/watchdog_kill_events."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from deployment_api.services.operational_data_writer import (
    write_idle_spend_snapshot,
    write_reap_event,
    write_watchdog_kill_event,
)

_PATCH_CLIENT = "deployment_api.services.operational_data_writer.get_analytics_client"


class TestWriteReapEvent:
    def test_skips_on_dry_run(self):
        with patch(_PATCH_CLIENT) as get_client:
            write_reap_event(
                "proj", vm_name="vm-1", age_hours=5.0, reclaimed_usd_per_month=10.0, actor="test", dry_run=True
            )
        get_client.assert_not_called()

    def test_inserts_one_row_on_real_delete(self):
        fake_client = MagicMock()
        with patch(_PATCH_CLIENT, return_value=fake_client):
            write_reap_event(
                "proj", vm_name="vm-1", age_hours=5.0, reclaimed_usd_per_month=10.0, actor="test", dry_run=False
            )
        fake_client.insert_rows.assert_called_once()
        args, kwargs = fake_client.insert_rows.call_args
        assert args[0] == "reap_events"
        assert args[1][0]["vm_name"] == "vm-1"
        assert kwargs["dataset"] == "deployment_operational_data"

    def test_swallows_insert_failure(self):
        fake_client = MagicMock()
        fake_client.insert_rows.side_effect = RuntimeError("bq down")
        with patch(_PATCH_CLIENT, return_value=fake_client):
            write_reap_event(  # must not raise
                "proj", vm_name="vm-1", age_hours=None, reclaimed_usd_per_month=0.0, actor="test", dry_run=False
            )


class TestWriteIdleSpendSnapshot:
    def test_writes_rollup_plus_per_resource_rows(self):
        fake_client = MagicMock()
        fake_client.insert_rows.return_value = 3
        with patch(_PATCH_CLIENT, return_value=fake_client):
            count = write_idle_spend_snapshot(
                "proj",
                stopped_total=2,
                reapable_total=1,
                monthly_idle_usd=20.0,
                monthly_reapable_usd=10.0,
                per_resource=[
                    {
                        "resource_name": "vm-a",
                        "lifecycle_class": "EPHEMERAL_BATCH",
                        "age_hours": 30.0,
                        "monthly_idle_usd": 10.0,
                        "monthly_reapable_usd": 10.0,
                    },
                    {
                        "resource_name": "vm-b",
                        "lifecycle_class": "PERMANENT",
                        "age_hours": 5.0,
                        "monthly_idle_usd": 10.0,
                        "monthly_reapable_usd": None,
                    },
                ],
            )
        assert count == 3
        args, kwargs = fake_client.insert_rows.call_args
        assert args[0] == "idle_spend"
        rows = args[1]
        assert len(rows) == 3  # 1 rollup + 2 per-resource
        assert rows[0]["resource_name"] is None
        assert rows[0]["stopped_total"] == 2
        assert rows[1]["resource_name"] == "vm-a"
        assert rows[1]["stopped_total"] is None

    def test_swallows_insert_failure_and_returns_zero(self):
        fake_client = MagicMock()
        fake_client.insert_rows.side_effect = RuntimeError("bq down")
        with patch(_PATCH_CLIENT, return_value=fake_client):
            count = write_idle_spend_snapshot(
                "proj",
                stopped_total=0,
                reapable_total=0,
                monthly_idle_usd=0.0,
                monthly_reapable_usd=0.0,
                per_resource=[],
            )
        assert count == 0


class TestWriteWatchdogKillEvent:
    def test_inserts_one_row_on_kill_event(self):
        fake_client = MagicMock()
        with patch(_PATCH_CLIENT, return_value=fake_client):
            write_watchdog_kill_event(
                "proj",
                vm_name="ao-host",
                pid=12345,
                slot_id="6",
                command="python3 /usr/bin/agent-orchestrator",
                reason="rss 8192 MB > limit 4096 MB",
                rss_mb=8192,
                limit_mb=4096,
                pressure_level="critical",
                killed=True,
            )
        fake_client.insert_rows.assert_called_once()
        args, kwargs = fake_client.insert_rows.call_args
        assert args[0] == "watchdog_kill_events"
        row = args[1][0]
        assert row["vm_name"] == "ao-host"
        assert row["pid"] == 12345
        assert row["slot_id"] == "6"
        assert row["rss_mb"] == 8192
        assert row["limit_mb"] == 4096
        assert row["killed"] is True
        assert kwargs["dataset"] == "deployment_operational_data"

    def test_swallows_insert_failure(self):
        fake_client = MagicMock()
        fake_client.insert_rows.side_effect = RuntimeError("bq down")
        with patch(_PATCH_CLIENT, return_value=fake_client):
            write_watchdog_kill_event(  # must not raise
                "proj",
                vm_name="ao-host",
                pid=0,
                slot_id="",
                command="",
                reason="test failure",
                rss_mb=0,
                limit_mb=0,
                pressure_level="normal",
                killed=False,
            )

    def test_handles_malformed_payload_gracefully(self):
        """A malformed payload (e.g. None for a required field) must not raise."""
        fake_client = MagicMock()
        fake_client.insert_rows.side_effect = TypeError("bad types")
        with patch(_PATCH_CLIENT, return_value=fake_client):
            write_watchdog_kill_event(  # must not raise — best-effort
                "proj",
                vm_name="ao-host",
                pid=-1,
                slot_id="",
                command="bad-cmd",
                reason="",
                rss_mb=-1,
                limit_mb=-1,
                pressure_level="unknown",
                killed=False,
            )
