"""
Unit tests for the repo-root gunicorn.conf.py — the file Dockerfile/Dockerfile.dashboard
actually ``COPY`` and reference via ``gunicorn ... -c /app/gunicorn.conf.py``. A prior
``deployment_api/gunicorn.conf.py`` duplicate existed and was where the leader-election
(``post_fork``) and faulthandler (``post_worker_init``) fixes were originally written —
but that file was NEVER loaded by either Dockerfile, so both fixes were silently inert in
production despite passing this same test suite (see
plans/active/issues/deployment_registry_reaper_not_draining_stale_entries_2026_07_24.md).
Deleted that dead file; this test now exercises the ONE file gunicorn actually loads.

The file is loaded by gunicorn as a config FILE (``gunicorn -c gunicorn.conf.py``), not
imported via a normal dotted path — its name (``gunicorn.conf.py``) embeds a dot, so
``import gunicorn.conf`` isn't a legal module path. Load it the same way, via
``importlib.util.spec_from_file_location``, to exercise it in isolation.
"""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from deployment_api.utils import worker_identity

_CONF_PATH = Path(__file__).parent.parent.parent / "gunicorn.conf.py"


def _load_gunicorn_conf():
    """Fresh-load the repo-root gunicorn.conf.py as an isolated module."""
    spec = importlib.util.spec_from_file_location("_test_gunicorn_conf", _CONF_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        del sys.modules[spec.name]
    return module


@pytest.fixture(autouse=True)
def _reset_worker_age(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker_identity, "_worker_age", None)


class TestPostFork:
    def test_records_worker_age_via_worker_identity(self) -> None:
        conf = _load_gunicorn_conf()
        server = MagicMock()
        worker = MagicMock()
        worker.age = 3

        conf.post_fork(server, worker)

        assert worker_identity._worker_age == 3

    def test_is_leader_worker_reflects_recorded_age(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(worker_identity, "WORKERS", 4)
        conf = _load_gunicorn_conf()
        server = MagicMock()
        worker = MagicMock()
        worker.age = 1

        conf.post_fork(server, worker)

        assert worker_identity.is_leader_worker() is True


class TestPostWorkerInit:
    def test_enables_faulthandler(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """deployment_api_sigabrt_crash_loop_2026_07_24 todo 3: faulthandler must be
        (re-)enabled in post_worker_init, not post_fork — a post_fork-only call gets
        silently uninstalled by UvicornWorker.init_signals()'s SIG_DFL reset loop
        before any real SIGABRT ever fires. See the hook's docstring for the full
        gunicorn/uvicorn call-order trace."""
        conf = _load_gunicorn_conf()
        enable_calls: list[bool] = []
        monkeypatch.setattr(conf.faulthandler, "enable", lambda: enable_calls.append(True))

        conf.post_worker_init(MagicMock())

        assert enable_calls == [True]

    def test_post_fork_no_longer_calls_faulthandler_enable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Regression guard: post_fork's faulthandler.enable() call is a no-op (silently
        uninstalled seconds later) — it must not be re-added there without also removing
        it from post_worker_init, which would just recreate this bug's exact symptom."""
        conf = _load_gunicorn_conf()
        enable_calls: list[bool] = []
        monkeypatch.setattr(conf.faulthandler, "enable", lambda: enable_calls.append(True))
        worker = MagicMock()
        worker.age = 0

        conf.post_fork(MagicMock(), worker)

        assert enable_calls == []


class TestModuleShape:
    def test_workers_setting_present(self) -> None:
        conf = _load_gunicorn_conf()
        assert isinstance(conf.workers, int)

    def test_worker_class_is_uvicorn(self) -> None:
        conf = _load_gunicorn_conf()
        assert conf.worker_class == "uvicorn.workers.UvicornWorker"
