"""
Gunicorn configuration for production deployment.

This config is used when running the API with gunicorn (production mode).
Gunicorn manages multiple uvicorn workers for better performance and reliability.

Environment variables:
    WORKERS: Number of worker processes (default: 4)
    PORT: Port to bind to (default: 8080)
"""

import faulthandler
from typing import Protocol

from deployment_api.settings import PORT as _PORT
from deployment_api.settings import WORKERS as _WORKERS
from deployment_api.utils.worker_identity import set_worker_age


class _GunicornLogger(Protocol):
    def info(self, msg: str, *args: object) -> None: ...


class _GunicornServer(Protocol):
    log: _GunicornLogger


class _GunicornWorker(Protocol):
    log: _GunicornLogger
    age: int


# Server socket
bind = f"0.0.0.0:{_PORT}"
backlog = 2048

# Worker processes
# Rule of thumb: 2-4 workers per CPU core
workers = _WORKERS
worker_class = "uvicorn.workers.UvicornWorker"
worker_connections = 1000
timeout = 300  # 5 minutes for turbo data-status (instruments-service 6yr x venues can be slow)
keepalive = 5

# Graceful restart settings
graceful_timeout = 30
max_requests = 1000  # Restart worker after N requests (prevents memory leaks)
max_requests_jitter = 100  # Randomize to prevent all workers restarting at once

# Logging
accesslog = "-"  # stdout
errorlog = "-"  # stderr
loglevel = "info"
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(D)s'

# Process naming
proc_name = "deployment-dashboard"

# Server mechanics
daemon = False
pidfile = None
umask = 0
user = None
group = None
tmp_upload_dir = None

# Preload app for faster worker startup (shares loaded code between workers)
preload_app = True


def pre_fork(server: _GunicornServer, worker: _GunicornWorker) -> None:
    """Called just before a worker is forked."""


def post_fork(server: _GunicornServer, worker: _GunicornWorker) -> None:
    """Called just after a worker has been forked.

    Records this worker's gunicorn-assigned age (spawn-order counter) in-process via
    ``worker_identity.set_worker_age`` so the ASGI app — once it starts inside this SAME
    forked process — can elect a single leader worker for singleton background tasks
    (see ``deployment_api.utils.worker_identity`` + ``deployment_api.lifespan``).

    ``faulthandler.enable()`` is deliberately NOT called here (see ``post_worker_init``
    below for why a call at this point is silently neutered).
    """
    set_worker_age(worker.age)


def post_worker_init(worker: _GunicornWorker) -> None:
    """Called after the worker is fully initialized (signals set up, WSGI/ASGI app
    loaded) — the LAST gunicorn hook before it enters its request-serving run loop.

    Enables ``faulthandler`` here, not in ``post_fork``. Root cause of the undiagnosed
    `Uncaught signal: 6` crash-loop with zero Python-level trace
    (plans/active/issues/deployment_api_sigabrt_crash_loop_2026_07_24.md): a
    ``faulthandler.enable()`` call in ``post_fork`` DOES install a C-level SIGABRT
    handler, but it is silently uninstalled moments later —
    ``Worker.init_process()`` (called by the Arbiter right after ``post_fork``) calls
    ``self.init_signals()``, and this app's ``worker_class`` is
    ``uvicorn.workers.UvicornWorker``, whose ``init_signals()`` override
    (``uvicorn/workers.py``) does ``for s in self.SIGNALS: signal.signal(s, signal.SIG_DFL)``
    — ``self.SIGNALS`` (``gunicorn/workers/base.py``) is
    ``[SIGABRT, SIGHUP, SIGQUIT, SIGINT, SIGTERM, SIGUSR1, SIGUSR2, SIGWINCH, SIGCHLD]``,
    which INCLUDES SIGABRT. So every worker's SIGABRT disposition is reset to the raw
    kernel default (``SIG_DFL``) microseconds after ``faulthandler.enable()`` runs —
    when the Arbiter's ``murder_workers()`` (or any other SIGABRT source) actually fires,
    there is no handler left to dump anything; the kernel just terminates the process,
    which is exactly what Cloud Run's sandbox reports as "Uncaught signal: 6" with zero
    Python-level trace. ``post_worker_init`` runs AFTER ``init_signals()`` (and after
    ``load_wsgi()``), and nothing downstream — uvicorn's own ``Server`` only ever
    installs handlers for SIGINT/SIGTERM (``uvicorn/server.py``'s ``HANDLED_SIGNALS``),
    never SIGABRT — touches SIGABRT's disposition again, so a ``faulthandler.enable()``
    call here actually sticks for the rest of the worker's life.
    """
    faulthandler.enable()


def pre_exec(server: _GunicornServer) -> None:
    """Called just before a new master process is forked."""
    server.log.info("Forked child, re-executing.")


def when_ready(server: _GunicornServer) -> None:
    """Called when the server is ready to accept connections."""
    server.log.info("Server is ready. Spawning workers")


def worker_int(worker: _GunicornWorker) -> None:
    """Called when a worker receives INT or QUIT signal."""
    worker.log.info("worker received INT or QUIT signal")


def worker_abort(worker: _GunicornWorker) -> None:
    """Called when a worker receives SIGABRT signal."""
    worker.log.info("worker received SIGABRT signal")
