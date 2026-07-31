"""
Gunicorn configuration for production deployment.

This config is used when running the API with gunicorn (production mode).
Gunicorn manages multiple uvicorn workers for better performance and reliability.

Environment variables:
    WORKERS: Number of worker processes (default: 4)
    PORT: Port to bind to (default: 8080)
"""

import faulthandler
import os

# Read directly from env to avoid triggering the full UTL→UAC import chain
# during gunicorn config loading (before the WSGI app is initialised).
_PORT = int(os.environ.get("PORT", 8080))  # config-bootstrap: gunicorn loads pre-UnifiedCloudConfig (Cloud Run PORT)
_WORKERS = int(os.environ.get("WORKERS", 4))  # config-bootstrap: gunicorn loads pre-UnifiedCloudConfig

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


def on_starting(server: object) -> None:  # pyright: ignore[reportAny]
    """Called ONCE, in the MASTER/arbiter process, before any worker is forked.

    Logs the arbiter's own pid so a future ``Uncaught signal: 6`` occurrence's pid can be
    looked up against this log line (and ``post_fork``'s per-worker line below) to confirm
    whether it belongs to the MASTER or a forked WORKER — without guessing from pid
    magnitude alone, per the low-pid (28/29) vs high-pid (280/900/5096) split found in
    plans/active/issues/deployment_api_sigabrt_crash_loop_2026_07_24.md.
    """
    server.log.info("gunicorn MASTER (arbiter) started, pid=%s", os.getpid())  # pyright: ignore[reportAttributeAccessIssue,reportUnknownMemberType]


def pre_fork(server: object, worker: object) -> None:  # pyright: ignore[reportAny]
    """Called just before a worker is forked."""
    pass


def post_fork(server: object, worker: object) -> None:  # pyright: ignore[reportAny]
    """Called just after a worker has been forked.

    Logs this worker's pid + gunicorn-assigned age (spawn-order counter) — paired with
    ``on_starting``'s master-pid line above, this gives a durable stdout/stderr record
    mapping every pid this container ever forks to a role (MASTER vs WORKER) and spawn
    generation, so the pid on the NEXT ``Uncaught signal: 6`` occurrence can be matched
    against these lines instead of inferred from pid magnitude alone (see this hook's
    ``on_starting`` sibling above for the full rationale).

    Also records the age in-process via ``worker_identity.set_worker_age`` so the ASGI
    app — once it starts inside this SAME forked process — can elect a single leader
    worker for singleton background tasks (see ``deployment_api.utils.worker_identity`` +
    ``deployment_api.lifespan``). Imported HERE (deferred, function-local) rather than at
    module level: by the time ``post_fork`` runs, ``preload_app``'s own import of the ASGI
    app has already succeeded once in the master and is cached in ``sys.modules``, so
    importing ``deployment_api.settings`` here is safe/cheap — a module-level import in
    THIS config file would instead run during gunicorn's config-file load, before the app
    is preloaded, and can hard-crash gunicorn startup on a missing env var (confirmed
    empirically — see the sibling ``deployment_api/gunicorn.conf.py`` dead-file finding
    in deployment_registry_reaper_not_draining_stale_entries_2026_07_24.md).

    ``faulthandler.enable()`` is deliberately NOT called here (see ``post_worker_init``
    below for why a call at this point is silently neutered).
    """
    server.log.info("gunicorn WORKER forked, pid=%s age=%s", worker.pid, worker.age)  # pyright: ignore[reportAttributeAccessIssue,reportUnknownMemberType,reportUnknownArgumentType]

    from deployment_api.utils.worker_identity import set_worker_age

    set_worker_age(worker.age)  # pyright: ignore[reportAttributeAccessIssue,reportUnknownMemberType,reportUnknownArgumentType]


def post_worker_init(worker: object) -> None:  # pyright: ignore[reportAny]
    """Called after the worker is fully initialized (signals set up, WSGI/ASGI app
    loaded) — the LAST gunicorn hook before it enters its request-serving run loop.

    Enables ``faulthandler`` here, not in ``post_fork``. Root cause of the undiagnosed
    `Uncaught signal: 6` crash-loop with zero Python-level trace
    (plans/active/issues/deployment_api_sigabrt_crash_loop_2026_07_24.md): a
    ``faulthandler.enable()`` call in ``post_fork`` DOES install a C-level SIGABRT
    handler, but it is silently uninstalled moments later — ``Worker.init_process()``
    (called by the Arbiter right after ``post_fork``) calls ``self.init_signals()``,
    and this app's ``worker_class`` is ``uvicorn.workers.UvicornWorker``, whose
    ``init_signals()`` override (``uvicorn/workers.py``) does
    ``for s in self.SIGNALS: signal.signal(s, signal.SIG_DFL)`` — ``Worker.SIGNALS``
    (``gunicorn/workers/base.py``) is
    ``[SIGABRT, SIGHUP, SIGQUIT, SIGINT, SIGTERM, SIGUSR1, SIGUSR2, SIGWINCH, SIGCHLD]``,
    which INCLUDES SIGABRT. So every worker's SIGABRT disposition is reset to the raw
    kernel default microseconds after ``faulthandler.enable()`` runs — by the time a
    real SIGABRT fires there is no handler left to dump anything. ``post_worker_init``
    runs AFTER ``init_signals()`` (and after ``load_wsgi()``), and nothing downstream —
    uvicorn's own ``Server`` only ever installs handlers for SIGINT/SIGTERM
    (``uvicorn/server.py``'s ``HANDLED_SIGNALS``) — touches SIGABRT's disposition again,
    so a ``faulthandler.enable()`` call here actually sticks for the rest of the
    worker's life.
    """
    faulthandler.enable()


def pre_exec(server: object) -> None:  # pyright: ignore[reportAny]
    """Called just before a new master process is forked."""
    server.log.info("Forked child, re-executing.")  # pyright: ignore[reportAttributeAccessIssue,reportUnknownMemberType]


def when_ready(server: object) -> None:  # pyright: ignore[reportAny]
    """Called when the server is ready to accept connections."""
    server.log.info("Server is ready. Spawning workers")  # pyright: ignore[reportAttributeAccessIssue,reportUnknownMemberType]


def worker_int(worker: object) -> None:  # pyright: ignore[reportAny]
    """Called when a worker receives INT or QUIT signal."""
    worker.log.info("worker received INT or QUIT signal")  # pyright: ignore[reportAttributeAccessIssue,reportUnknownMemberType]


def worker_abort(worker: object) -> None:  # pyright: ignore[reportAny]
    """Called when a worker receives SIGABRT signal."""
    worker.log.info("worker received SIGABRT signal")  # pyright: ignore[reportAttributeAccessIssue,reportUnknownMemberType]
