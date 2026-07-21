"""Unit tests for the sports coverage-drift snapshot + comparator worker (Phase 8.B cron).

Plan: features_sports_honest_coverage_2026_05_05.md, Phase 8.B.
Issue: features_sports_deployment_ui_coverage_tab_and_registry_playbook_2026_07_21.md, todo 3.
"""

from __future__ import annotations

import io
from unittest.mock import patch

import pandas as pd

from deployment_api.scripts import coverage_drift_worker as worker


def _snapshot_bytes(rows: list[dict[str, object]]) -> bytes:
    buf = io.BytesIO()
    pd.DataFrame(rows).to_parquet(buf, index=False)
    return buf.getvalue()


class TestSnapshotBlobPath:
    def test_path_is_date_partitioned_under_the_sports_prefix(self) -> None:
        assert worker.snapshot_blob_path("2026-07-21") == "coverage-drift-snapshots/sports/2026-07-21.parquet"


class TestSportsManifestSnapshot:
    def test_projects_known_calculators_only(self) -> None:
        idx = pd.DataFrame(
            [
                {"feature_group": "team_form", "league_id": "EPL", "capture_status": "captured"},
                # Not a known calculator (e.g. a removed/residual entity) — must be dropped.
                {"feature_group": "SFI_STANDINGS", "league_id": "EPL", "capture_status": "captured"},
            ]
        )
        with (
            patch.object(worker, "resolve_bucket_name", return_value="features-sports-test-project"),
            patch.object(worker, "read_manifest_index", return_value=idx),
            patch.object(worker, "known_calculators", return_value={"team_form"}),
        ):
            out = worker._sports_manifest_snapshot()  # pyright: ignore[reportPrivateUsage]
        assert list(out["feature_group"]) == ["team_form"]

    def test_empty_manifest_returns_empty_typed_frame(self) -> None:
        with (
            patch.object(worker, "resolve_bucket_name", return_value="features-sports-test-project"),
            patch.object(worker, "read_manifest_index", return_value=pd.DataFrame()),
        ):
            out = worker._sports_manifest_snapshot()  # pyright: ignore[reportPrivateUsage]
        assert out.empty
        assert list(out.columns) == list(worker.SNAPSHOT_COLUMNS)


class TestWriteSnapshot:
    def test_writes_parquet_to_the_state_bucket_prefix(self) -> None:
        written: dict[str, object] = {}

        def _fake_upload(bucket: str, path: str, data: bytes, content_type: str | None = None) -> str:
            written["bucket"] = bucket
            written["path"] = path
            written["data"] = data
            return f"gs://{bucket}/{path}"

        snap = pd.DataFrame([{"feature_group": "team_form", "league_id": "EPL", "capture_status": "captured"}])
        with (
            patch.object(worker, "_sports_manifest_snapshot", return_value=snap),
            patch.object(worker, "upload_to_storage", side_effect=_fake_upload),
        ):
            result = worker.write_snapshot("test-bucket", "2026-07-21")

        assert result["ok"] is True
        assert result["rows"] == 1
        assert written["bucket"] == "test-bucket"
        assert written["path"] == "coverage-drift-snapshots/sports/2026-07-21.parquet"

    def test_degrades_to_a_result_dict_on_failure_never_raises(self) -> None:
        with patch.object(worker, "_sports_manifest_snapshot", side_effect=RuntimeError("gcs unavailable")):
            result = worker.write_snapshot("test-bucket", "2026-07-21")
        assert result["ok"] is False
        assert "gcs unavailable" in str(result["error"])


class TestReadSnapshot:
    def test_returns_none_when_blob_absent(self) -> None:
        with patch.object(worker, "storage_exists", return_value=False):
            assert worker._read_snapshot("test-bucket", "2026-07-01") is None  # pyright: ignore[reportPrivateUsage]

    def test_roundtrips_a_written_snapshot(self) -> None:
        raw = _snapshot_bytes([{"feature_group": "team_form", "league_id": "EPL", "capture_status": "captured"}])
        with (
            patch.object(worker, "storage_exists", return_value=True),
            patch.object(worker, "download_from_storage", return_value=raw),
        ):
            out = worker._read_snapshot("test-bucket", "2026-07-14")  # pyright: ignore[reportPrivateUsage]
        assert out is not None
        assert list(out["feature_group"]) == ["team_form"]


class TestRunDriftCheck:
    def test_no_alert_when_todays_snapshot_missing(self) -> None:
        with (
            patch.object(worker, "_read_snapshot", return_value=None),
            patch.object(worker, "_persist_alert") as mock_persist,
        ):
            events = worker.run_drift_check("test-bucket", "2026-07-21")
        assert events == []
        mock_persist.assert_not_called()

    def test_no_alert_when_no_baseline_snapshot_yet(self) -> None:
        today_df = pd.DataFrame([{"feature_group": "team_form", "league_id": "EPL", "capture_status": "captured"}])

        def _fake_read(_bucket: str, date: str) -> pd.DataFrame | None:
            return today_df if date == "2026-07-21" else None

        with (
            patch.object(worker, "_read_snapshot", side_effect=_fake_read),
            patch.object(worker, "_persist_alert") as mock_persist,
        ):
            events = worker.run_drift_check("test-bucket", "2026-07-21", lookback_days=7)
        assert events == []
        mock_persist.assert_not_called()

    def test_persists_one_alert_per_drift_event_above_threshold(self) -> None:
        previous_df = pd.DataFrame(
            [{"feature_group": "team_form", "league_id": "EPL", "capture_status": "captured"}] * 10
        )
        # 10/10 -> 4/10 = 60pt drop, well above the 5pt threshold.
        current_df = pd.DataFrame(
            [{"feature_group": "team_form", "league_id": "EPL", "capture_status": "captured"}] * 4
            + [{"feature_group": "team_form", "league_id": "EPL", "capture_status": "attempted_failed"}] * 6
        )

        def _fake_read(_bucket: str, date: str) -> pd.DataFrame | None:
            return current_df if date == "2026-07-21" else previous_df

        with (
            patch.object(worker, "_read_snapshot", side_effect=_fake_read),
            patch.object(worker, "_persist_alert") as mock_persist,
        ):
            events = worker.run_drift_check("test-bucket", "2026-07-21", lookback_days=7)

        assert len(events) == 1
        assert events[0].calc == "team_form"
        mock_persist.assert_called_once()
        kwargs = mock_persist.call_args.kwargs
        assert kwargs["alert_class"] == "coverage_drift"
        assert kwargs["dedup_key"] == "coverage-drift-team_form-EPL-2026-07-21"
        assert "team_form" in kwargs["message"]


class TestRun:
    def test_orchestrates_write_then_drift_check(self) -> None:
        write_result = {"ok": True, "rows": 3, "bytes": 100}
        fake_event = worker.DriftEvent(
            calc="team_form", league_id="EPL", previous_coverage_pct=90.0, current_coverage_pct=60.0, drift_pct=30.0
        )
        with (
            patch.object(worker, "write_snapshot", return_value=write_result) as mock_write,
            patch.object(worker, "run_drift_check", return_value=[fake_event]) as mock_check,
        ):
            result = worker.run(lookback_days=7)

        mock_write.assert_called_once()
        mock_check.assert_called_once()
        assert result["write"] == write_result
        assert result["drift_event_count"] == 1
        assert "team_form" in result["drift_events"][0]

    def test_skips_drift_check_when_write_fails(self) -> None:
        with (
            patch.object(worker, "write_snapshot", return_value={"ok": False, "error": "boom"}),
            patch.object(worker, "run_drift_check") as mock_check,
        ):
            result = worker.run()
        mock_check.assert_not_called()
        assert result["drift_event_count"] == 0


def test_coverage_drift_run_endpoint_dispatches_to_the_worker_in_service() -> None:
    """POST /api/data-status/sports/coverage-drift-run runs the worker's run() IN the gen1
    service (mirrors _rollup.py's rollup-run route — the working pattern for scheduled
    compute in this repo)."""
    import asyncio

    from deployment_api.routes.data_status import _coverage_drift_run

    captured: dict[str, object] = {}

    def _fake_run(lookback_days: int) -> dict[str, object]:
        captured["lookback_days"] = lookback_days
        return {"date": "2026-07-21", "write": {"ok": True}, "drift_events": [], "drift_event_count": 0}

    with patch.object(worker, "run", side_effect=_fake_run):
        out = asyncio.run(_coverage_drift_run.run_sports_coverage_drift(lookback_days=7))

    assert captured["lookback_days"] == 7
    assert out["drift_event_count"] == 0
