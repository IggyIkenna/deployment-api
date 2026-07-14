"""
Unit tests for deployment_api/gunicorn.conf.py.

The file is loaded by gunicorn as a config FILE (``gunicorn -c deployment_api/gunicorn.conf.py``),
not imported via a normal dotted path — its name (``gunicorn.conf.py``) embeds a dot, so
``import deployment_api.gunicorn.conf`` isn't a legal module path. Load it the same way,
via ``importlib.util.spec_from_file_location``, to exercise it in isolation.
"""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from deployment_api.utils import worker_identity

_CONF_PATH = Path(__file__).parent.parent.parent / "deployment_api" / "gunicorn.conf.py"


def _load_gunicorn_conf():
    """Fresh-load deployment_api/gunicorn.conf.py as an isolated module."""
    spec = importlib.util.spec_from_file_location("deployment_api._test_gunicorn_conf", _CONF_PATH)
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


class TestModuleShape:
    def test_workers_setting_present(self) -> None:
        conf = _load_gunicorn_conf()
        assert isinstance(conf.workers, int)

    def test_worker_class_is_uvicorn(self) -> None:
        conf = _load_gunicorn_conf()
        assert conf.worker_class == "uvicorn.workers.UvicornWorker"
