"""In-process worker-leader election for gunicorn multi-worker deployments.

Cloud Run runs this service under gunicorn with ``WORKERS`` forked ``UvicornWorker``
processes sharing one instance (see ``gunicorn.conf.py``). Singleton background work —
today the auto-sync loop started in ``deployment_api.lifespan`` (and the reap_stale tick
riding inside it) — must run on exactly ONE of those workers per instance, not all of
them: N duplicate loops independently scanning the same GCS deployments registry and
contending on the same per-deployment locks is pure waste, not extra throughput.

This module is the "simpler in-process signal" the election uses: gunicorn's own
``Worker.age`` (a process-lifetime-unique, monotonically increasing spawn counter — see
``gunicorn.arbiter.Arbiter.spawn_worker``) is recorded once, in-process, by
``gunicorn.conf.py``'s ``post_fork`` hook. ``post_fork`` runs in the freshly-forked
child — the SAME OS process the ASGI app and its ``lifespan`` later run in (gunicorn
does not fork again after ``post_fork``) — so a plain module-level variable set there is
still set when ``lifespan`` reads it later. No lock file, no external election service:
everything here is scoped to one process's own memory.

Leadership rotates by ``age modulo worker_count`` rather than pinning to a fixed
``age == 1`` so it self-heals: ``Worker.age`` is never reused within one gunicorn
arbiter's lifetime, so when gunicorn recycles the current leader (``max_requests``
exhaustion or a crash) that exact age never runs again — but the replacement worker's
age lands on the same residue class mod ``worker_count``, so leadership picks back up
automatically instead of leaving the instance leaderless for the rest of its life.
"""

from deployment_api.settings import WORKERS

_worker_age: int | None = None


def set_worker_age(age: int) -> None:
    """Record this process's gunicorn worker age. Call once, from ``post_fork``."""
    global _worker_age
    _worker_age = age


def is_leader_worker() -> bool:
    """True iff this process is the elected leader for singleton background tasks.

    No gunicorn arbiter in play — plain ``uvicorn`` (local dev, tests) or ``WORKERS<=1``
    — means ``_worker_age`` was never set (or there is only one worker to begin with):
    this process is the only worker, so it is always the leader.
    """
    if _worker_age is None or WORKERS <= 1:
        return True
    return (_worker_age - 1) % WORKERS == 0
