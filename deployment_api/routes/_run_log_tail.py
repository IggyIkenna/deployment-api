# Epic: observability_master
# Lifecycle: permanent
"""Bounded tail read of a VM's run.log — byte-range GCS read, split to a line cap.

Reads at most ``max_bytes`` from the END of the resolved run.log object (via
``gcs_read_object_range``, never the full object — observed sizes run 362KB-13.4MB, a
worst case of 20-30MB is plausible) and splits that bounded slice into at most
``max_lines`` lines. When the byte-range read doesn't start at byte 0 of the object, its
first fragment is a partial line cut from the middle of an earlier line — dropped rather
than shown truncated.
"""

from __future__ import annotations

from unified_trading_library import gcs_read_object_range


def tail_lines_from_bytes(raw: bytes, max_lines: int, *, drop_leading_partial: bool) -> list[str]:
    """Split a raw byte slice into its last ``max_lines`` lines.

    ``drop_leading_partial=True`` drops the fragment before the first ``\\n`` (the read
    started mid-line, e.g. a byte-range read whose ``start`` is not the object's byte 0).
    A single trailing empty string from a terminal ``\\n`` is always stripped so the log's
    last real line is what shows, not a blank one.
    """
    if not raw:
        return []
    text = raw.decode("utf-8", errors="replace")
    parts = text.split("\n")
    if drop_leading_partial and len(parts) > 1:
        parts = parts[1:]
    if parts and parts[-1] == "":
        parts = parts[:-1]
    return parts[-max_lines:] if max_lines > 0 else []


def read_run_log_tail(uri: str, size_bytes: int, *, max_bytes: int, max_lines: int) -> tuple[list[str], int]:
    """Read + split the last ``max_lines`` lines of the run.log object at ``uri``.

    ``size_bytes`` is the object's total size (from a prior ``gcs_describe_object`` call
    via ``resolve_run_log_location`` — never re-fetched here). Returns
    ``(lines, tail_bytes_read)``. At most ``max_bytes`` are ever read from GCS,
    independent of ``size_bytes``.
    """
    if size_bytes <= 0:
        return [], 0
    start = max(0, size_bytes - max_bytes)
    raw = gcs_read_object_range(uri, start, size_bytes)
    lines = tail_lines_from_bytes(raw, max_lines, drop_leading_partial=start > 0)
    return lines, len(raw)
