"""Tests for POST /api/backfill/launch (mocked-subprocess, no GCP creds).

Coverage matrix (per work-stream-A plan §2.A):
  1. Auth — POST without X-API-Key → 401 (covered by FastAPI auth dep).
  2. Bad task — request with task value not in BackfillLaunchTaskKind → 422.
  3. Unknown VM-name prefix — task whose resolved prefix isn't in the
     watchdog registry → 400 with `UNREGISTERED_VM_PREFIX`.
  4. Dry-run — returns BackfillLaunchResult{dry_run=True}; argv shape matches
     the launcher contract; vm_name regex `^[a-z][-a-z0-9]+-\\d{8}-\\d{6}$`;
     events_uri matches `gs://{pid}-events/events/{service}/{date}/{vm}/`;
     subprocess.run NEVER called.
  5. Mocked subprocess — real-mode happy path. subprocess.run intercepted via
     monkeypatch; returns 0; route returns BackfillLaunchResult{dry_run=False}
     with the right argv + env asserted via the captured call.
  6. subprocess timeout → 504 with LAUNCH_TIMEOUT envelope.
  7. subprocess non-zero exit → 502 with VM_LAUNCH_FAILED envelope.
"""

from __future__ import annotations

import os
import re
import subprocess
from collections.abc import Iterator

os.environ.setdefault("CLOUD_MOCK_MODE", "true")
os.environ.setdefault("CLOUD_PROVIDER", "local")
os.environ.setdefault("GCP_PROJECT_ID", "test-project")
os.environ.setdefault("DISABLE_AUTH", "true")
os.environ.setdefault("MOCK_STATE_MODE", "deterministic")

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

with (
    patch("unified_trading_library.event_sink.PubSubEventSink"),
    patch("unified_trading_library.PubSubEventSink"),
    patch("unified_trading_library.events.setup_events"),
    patch("unified_trading_library.utils.tracing.setup_tracing"),
    patch("unified_trading_library.setup_tracing"),
):
    from deployment_api.main import app

from unified_trading_library import setup_events

setup_events("deployment-api", "test")

# `auth.DISABLE_AUTH` is frozen at module-import time from the env. When this
# test file is collected AFTER `auth.py` is already imported (e.g. by another
# test that imported `deployment_api.main` first), the env var setdefaults
# above are too late. Override the module attribute directly so verify_api_key
# resolves to "dev-mode" for these tests regardless of import order.
from deployment_api import auth as _auth_mod

_auth_mod.DISABLE_AUTH = True

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.routes import backfill_launch as backfill_launch_module

pytestmark = [pytest.mark.timeout(60)]


# Canonical VM-name regex per the plan §2.A.4: lowercase prefix segment +
# dashed digits suffix. Used to assert generated names match the workspace
# convention.
_VM_NAME_REGEX = re.compile(r"^[a-z][-a-z0-9]+-\d{8}-\d{6}$")


@pytest.fixture(scope="module")
def client() -> TestClient:
    return TestClient(app)


@pytest.fixture
def disable_mock_mode(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Force the route into real-mode (subprocess path).

    Pydantic blocks instance attribute assignment for methods, so we patch
    the class — `_cfg.is_mock_mode()` then resolves through the patched
    method. monkeypatch reverts the class-level patch after each test.
    """
    monkeypatch.setattr(DeploymentApiConfig, "is_mock_mode", lambda self: False)
    yield


@pytest.fixture
def fake_launcher_script(tmp_path, monkeypatch: pytest.MonkeyPatch) -> str:
    """Stand up a tmp `deployment-service/scripts/vm/launch-X.sh` so the
    route's `launcher_path.is_file()` check passes in real-mode tests.
    """
    vm_dir = tmp_path / "deployment-service" / "scripts" / "vm"
    vm_dir.mkdir(parents=True)
    # Touch one launcher so MDPS_BACKFILL real-mode tests resolve a real path.
    launcher = vm_dir / "launch-mdps-backfill-vm.sh"
    launcher.write_text("#!/usr/bin/env bash\necho fake-launcher\n")
    monkeypatch.setattr(backfill_launch_module._cfg, "workspace_root", str(tmp_path))
    return str(launcher)


# ---------------------------------------------------------------------------
# Validation + prefix registry
# ---------------------------------------------------------------------------


class TestRequestValidation:
    def test_bad_task_returns_422(self, client: TestClient) -> None:
        """Pydantic enum validation on `task` rejects unknown values."""
        resp = client.post(
            "/api/backfill/launch",
            json={
                "task": "not_a_real_task",
                "asset_group": "cefi",
                "extra_metadata": {},
            },
        )
        assert resp.status_code == 422

    def test_missing_required_field_returns_422(self, client: TestClient) -> None:
        """asset_group is required."""
        resp = client.post(
            "/api/backfill/launch",
            json={
                "task": "mdps_backfill",
                "extra_metadata": {},
            },
        )
        assert resp.status_code == 422

    def test_unregistered_prefix_returns_400(self, client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
        """Forge a launcher spec whose resolved prefix isn't in the registry."""
        # Inject a temp task→spec entry whose prefix template renders to an
        # unregistered string. We monkey-patch the dict in place.
        from unified_api_contracts.internal import BackfillLaunchTaskKind

        forged = backfill_launch_module._LauncherSpec(
            launcher_filename="launch-fake-vm.sh",
            vm_prefix_template="totally-fake-prefix",
            service="market-tick-data-service",
        )
        monkeypatch.setitem(
            backfill_launch_module._TASK_TO_LAUNCHER,
            BackfillLaunchTaskKind.MDPS_BACKFILL,
            forged,
        )
        resp = client.post(
            "/api/backfill/launch",
            json={
                "task": "mdps_backfill",
                "asset_group": "cefi",
                "start_date": "2024-06-01",
                "end_date": "2024-06-30",
                "dry_run": True,
                "extra_metadata": {},
            },
        )
        assert resp.status_code == 400
        body = resp.json()
        assert body["error"]["code"] == "HTTP_400"
        # The standard error handler nests our detail under details.
        assert body["error"]["details"]["code"] == "UNREGISTERED_VM_PREFIX"
        assert "totally-fake-prefix-" in body["error"]["details"]["prefix"]


# ---------------------------------------------------------------------------
# Dry-run path (no subprocess)
# ---------------------------------------------------------------------------


class TestDryRun:
    def test_dry_run_mdps_backfill(self, client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
        """Happy path: dry_run=True → stub result, no subprocess call."""
        spy: dict[str, object] = {"called": False}

        def _no_subprocess(*_args: object, **_kwargs: object) -> object:
            spy["called"] = True
            raise AssertionError("subprocess.run must NOT be called in dry_run mode")

        monkeypatch.setattr(subprocess, "run", _no_subprocess)
        resp = client.post(
            "/api/backfill/launch",
            json={
                "task": "mdps_backfill",
                "asset_group": "cefi",
                "start_date": "2024-06-01",
                "end_date": "2024-06-30",
                "dry_run": True,
                "extra_metadata": {},
            },
        )
        assert resp.status_code == 200, resp.json()
        body = resp.json()
        assert body["dry_run"] is True
        assert body["vm_name_prefix"] == "mdps-backfill-cefi"
        assert _VM_NAME_REGEX.match(body["vm_name"]), body["vm_name"]
        assert body["vm_name"].startswith("mdps-backfill-cefi-")
        assert body["zone"] == "asia-northeast1-c"
        assert body["launcher_script"] == "launch-mdps-backfill-vm.sh"
        assert body["events_uri"].startswith("gs://test-project-events/events/")
        assert "/market-data-processing-service/" in body["events_uri"]
        assert body["events_uri"].endswith(f"/{body['vm_name']}/")
        # argv shape: ["bash", "<launcher_path>", "--dry-run"]
        assert body["argv"][0] == "bash"
        assert body["argv"][-1] == "--dry-run"
        assert body["argv"][1].endswith("/launch-mdps-backfill-vm.sh")
        # env_diff: workspace-mandatory keys always present.
        env_diff = body["env_diff"]
        assert env_diff["VM_NAME"] == body["vm_name"]
        assert env_diff["MANIFEST_PER_VM_SHARDS"] == "true"
        assert env_diff["VM_ASSET_GROUP"] == "CEFI"
        assert env_diff["VM_START_DATE"] == "2024-06-01"
        assert env_diff["VM_END_DATE"] == "2024-06-30"
        # correlation_id is a uuid4-shaped string
        assert len(body["correlation_id"]) == 36
        assert spy["called"] is False

    def test_dry_run_cefi_backfill_prefix_uses_venue(self, client: TestClient) -> None:
        """vm_prefix_template `cefi-{venue}` requires the venue field."""
        resp = client.post(
            "/api/backfill/launch",
            json={
                "task": "cefi_backfill",
                "asset_group": "cefi",
                "venue": "binance",
                "start_date": "2024-06-01",
                "end_date": "2024-06-30",
                "dry_run": True,
                "extra_metadata": {},
            },
        )
        assert resp.status_code == 200, resp.json()
        body = resp.json()
        assert body["vm_name_prefix"] == "cefi-binance"
        assert body["vm_name"].startswith("cefi-binance-")
        # Forced-flag propagation
        assert "VM_VENUE" in body["env_diff"]
        assert body["env_diff"]["VM_VENUE"] == "binance"

    def test_dry_run_force_flag_propagates(self, client: TestClient) -> None:
        """force=True → argv includes --force AND env VM_FORCE=true."""
        resp = client.post(
            "/api/backfill/launch",
            json={
                "task": "mdps_backfill",
                "asset_group": "tradfi",
                "start_date": "2024-01-01",
                "end_date": "2024-12-31",
                "force": True,
                "dry_run": True,
                "extra_metadata": {},
            },
        )
        assert resp.status_code == 200, resp.json()
        body = resp.json()
        assert "--force" in body["argv"]
        assert body["env_diff"]["VM_FORCE"] == "true"
        assert body["vm_name_prefix"] == "mdps-backfill-tradfi"

    def test_dry_run_extra_metadata_uppercased_and_prefixed(self, client: TestClient) -> None:
        """extra_metadata keys are uppercased + VM_-prefixed if absent."""
        resp = client.post(
            "/api/backfill/launch",
            json={
                "task": "canonical_migration",
                "asset_group": "defi",
                "force": False,
                "dry_run": True,
                "extra_metadata": {"some_key": "value-1", "VM_ALREADY": "value-2"},
            },
        )
        assert resp.status_code == 200, resp.json()
        body = resp.json()
        env_diff = body["env_diff"]
        assert env_diff["VM_SOME_KEY"] == "value-1"
        assert env_diff["VM_ALREADY"] == "value-2"


# ---------------------------------------------------------------------------
# Real-mode (subprocess) path — mocked
# ---------------------------------------------------------------------------


class TestRealModeMockedSubprocess:
    def test_real_mode_happy_path(
        self,
        client: TestClient,
        disable_mock_mode: None,
        fake_launcher_script: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Real-mode launch with subprocess.run monkeypatched returns dry_run=False."""
        captured: dict[str, object] = {}

        def _fake_run(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
            captured["argv"] = argv
            captured["env"] = kwargs.get("env", {})
            captured["timeout"] = kwargs.get("timeout")
            captured["shell"] = kwargs.get("shell")
            return subprocess.CompletedProcess(args=argv, returncode=0, stdout="ok\n", stderr="")

        monkeypatch.setattr(subprocess, "run", _fake_run)
        resp = client.post(
            "/api/backfill/launch",
            json={
                "task": "mdps_backfill",
                "asset_group": "cefi",
                "start_date": "2024-06-01",
                "end_date": "2024-06-30",
                "dry_run": False,
                "extra_metadata": {},
            },
        )
        assert resp.status_code == 200, resp.json()
        body = resp.json()
        assert body["dry_run"] is False
        # subprocess was called with argv = ["bash", launcher_path]
        # (no --force / --dry-run flags since neither was set on the request).
        assert captured["argv"] == ["bash", fake_launcher_script]
        assert captured["timeout"] == 600
        assert captured["shell"] is False
        # env was layered on top of os.environ, including our additions
        env = captured["env"]
        assert isinstance(env, dict)
        assert env["VM_NAME"] == body["vm_name"]
        assert env["MANIFEST_PER_VM_SHARDS"] == "true"
        assert env["VM_ASSET_GROUP"] == "CEFI"

    def test_real_mode_subprocess_timeout_returns_504(
        self,
        client: TestClient,
        disable_mock_mode: None,
        fake_launcher_script: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def _timeout_run(argv: list[str], **kwargs: object) -> object:
            raise subprocess.TimeoutExpired(cmd=argv, timeout=600)

        monkeypatch.setattr(subprocess, "run", _timeout_run)
        resp = client.post(
            "/api/backfill/launch",
            json={
                "task": "mdps_backfill",
                "asset_group": "cefi",
                "start_date": "2024-06-01",
                "end_date": "2024-06-30",
                "dry_run": False,
                "extra_metadata": {},
            },
        )
        assert resp.status_code == 504, resp.json()
        assert resp.json()["error"]["details"]["code"] == "LAUNCH_TIMEOUT"

    def test_real_mode_subprocess_failure_returns_502(
        self,
        client: TestClient,
        disable_mock_mode: None,
        fake_launcher_script: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def _fail_run(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
            return subprocess.CompletedProcess(
                args=argv,
                returncode=2,
                stdout="",
                stderr="ERROR: cefi-sharded-backfill VMs already running\n",
            )

        monkeypatch.setattr(subprocess, "run", _fail_run)
        resp = client.post(
            "/api/backfill/launch",
            json={
                "task": "mdps_backfill",
                "asset_group": "cefi",
                "start_date": "2024-06-01",
                "end_date": "2024-06-30",
                "dry_run": False,
                "extra_metadata": {},
            },
        )
        assert resp.status_code == 502, resp.json()
        body = resp.json()
        assert body["error"]["details"]["code"] == "VM_LAUNCH_FAILED"
        assert body["error"]["details"]["exit_code"] == 2

    def test_real_mode_missing_launcher_returns_500(
        self,
        client: TestClient,
        disable_mock_mode: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
    ) -> None:
        """Real-mode: if the launcher file doesn't exist, return 500
        LAUNCHER_SCRIPT_MISSING (catches misconfigured workspace_root).
        """
        # Empty workspace — launcher won't exist
        (tmp_path / "deployment-service" / "scripts" / "vm").mkdir(parents=True)
        monkeypatch.setattr(backfill_launch_module._cfg, "workspace_root", str(tmp_path))

        def _no_subprocess(*_args: object, **_kwargs: object) -> object:
            raise AssertionError("subprocess.run must NOT be called when launcher is missing")

        monkeypatch.setattr(subprocess, "run", _no_subprocess)
        resp = client.post(
            "/api/backfill/launch",
            json={
                "task": "mdps_backfill",
                "asset_group": "cefi",
                "start_date": "2024-06-01",
                "end_date": "2024-06-30",
                "dry_run": False,
                "extra_metadata": {},
            },
        )
        assert resp.status_code == 500, resp.json()
        assert resp.json()["error"]["details"]["code"] == "LAUNCHER_SCRIPT_MISSING"
