"""Cheap peak-RSS delta instrumentation for memory-suspect dashboard handlers.

``deployment_api_sigabrt_crash_loop_2026_07_24.md``'s OOM-kill sub-issue traced both
confirmed ``Container terminated on signal 9`` events to a cold multi-panel "cockpit"
dashboard-load burst (``/api/health/overview``, ``/api/repo-ci/overview``,
``/api/vm-deployments``), but could not attribute the memory spike to any ONE of those
three handlers without profiling data — this exists so the next occurrence's logs carry
that attribution instead of requiring another investigation to re-derive it.

``resource.getrusage(RUSAGE_SELF).ru_maxrss`` is the process's peak resident-set size in
KiB (Linux), monotonic non-decreasing since process start — it cannot isolate a precise
per-request allocation, but a jump between the value read at request-start and
request-end attributes NEW peak memory to whatever ran in between, which is the cheap
signal the parent issue's own next-step explicitly asked for (log RSS delta / use
``resource.getrusage``, not profile-and-guess).
"""

from __future__ import annotations

import logging
import resource
import time
from collections.abc import Iterator
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# Below this delta, don't log at WARNING — routine handler calls allocate some noise
# (pydantic model construction, JSON payloads) that would bury the real signal in Cloud
# Logging. 20 MiB is comfortably above that per-request noise floor while still well
# under the ~0.06%-4%-over-16384MiB margins the parent issue measured for the real
# OOM events, so a genuine contributor should still cross it.
_WARN_THRESHOLD_KIB = 20 * 1024


@contextmanager
def log_rss_delta(handler_name: str) -> Iterator[None]:
    """Log the peak-RSS delta (KiB) and wall time around a memory-suspect handler.

    Always logs at DEBUG (cheap, correlatable after the fact); escalates to WARNING
    when the delta crosses ``_WARN_THRESHOLD_KIB`` so a future SIGKILL's preceding
    request-log window surfaces the offending handler directly, without a manual
    per-handler grep across every cockpit tile.
    """
    start_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    start = time.monotonic()
    try:
        yield
    finally:
        end_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        delta_kib = end_rss - start_rss
        elapsed_s = time.monotonic() - start
        level = logging.WARNING if delta_kib >= _WARN_THRESHOLD_KIB else logging.DEBUG
        logger.log(
            level,
            "memory-profile %s: peak_rss_delta_kib=%d elapsed_s=%.2f peak_rss_kib=%d",
            handler_name,
            delta_kib,
            elapsed_s,
            end_rss,
        )


__all__ = ["log_rss_delta"]
