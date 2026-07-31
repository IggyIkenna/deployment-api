# Epic: observability_master
# Lifecycle: permanent
"""``health_consolidator`` per-bucket cheap-read helpers — split out of the facade (2026-07-31).

Every function here takes an already-resolved ``StorageClient`` + bucket/path as a plain
argument (never the module-level ``get_storage_client()`` factory), so none of these are a
seam the test suite patches by module-qualified name — safe to live in their own module
regardless of which submodule calls them.
"""

from __future__ import annotations

import io
import json
import logging
from typing import TYPE_CHECKING, TypedDict

from unified_trading_library import StorageClient

if TYPE_CHECKING:
    from _typeshed import WriteableBuffer

logger = logging.getLogger(__name__)

_INDEX_BLOB = "_index/availability_index.parquet"
_LATEST_RUN_BLOB = "_index/latest.json"  # the consolidator's self-published run summary (UTL manifest_consolidator)
# Dark data-correctness actors' self-published per-AG summaries (instruments-service phantom reconcile +
# e2e-testing empty re-probe), written to the SAME bucket the consolidator card reads. Absent = "no audit
# yet" (honest). Only market-data / instruments buckets carry these; other kinds read None (harmless).
_PHANTOM_AUDIT_BLOB = "_index/phantom_audit_latest.json"
_REPROBE_AUDIT_BLOB = "_index/reprobe_audit_latest.json"
# The kinds an audit targets — gate the two extra reads to these so features/execution/flat entries
# don't pay for a metadata HEAD that will always miss.
_AUDIT_BEARING_KINDS = frozenset({"market-data", "instruments"})


class _RangedIndexReader(io.RawIOBase):
    """Seekable read-only view over a GCS blob via ranged reads, so pyarrow can read just the
    parquet footer (``num_rows``) instead of downloading a multi-hundred-MB index — a 219 MB index
    yields its 14 M row count from ~490 KB of footer (verified 2026-07-10)."""

    def __init__(self, client: StorageClient, bucket: str, path: str, size: int) -> None:
        super().__init__()
        self._client = client
        self._bucket = bucket
        self._path = path
        self._size = size
        self._pos = 0

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            self._pos = offset
        elif whence == io.SEEK_CUR:
            self._pos += offset
        elif whence == io.SEEK_END:
            self._pos = self._size + offset
        return self._pos

    def tell(self) -> int:
        return self._pos

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def readinto(self, buffer: WriteableBuffer) -> int:
        view = memoryview(buffer).cast("B")
        end = min(self._size, self._pos + len(view))
        if end <= self._pos:
            return 0
        data = self._client.download_bytes_range(self._bucket, self._path, self._pos, end)
        n = len(data)
        view[:n] = data
        self._pos += n
        return n


def _read_index_json(client: StorageClient, bucket: str, blob: str) -> dict[str, object] | None:
    """Read a small self-published ``_index/*.json`` summary object, or ``None`` if absent/malformed.

    Absent = the producer has never run under the summary-emitting code → the cockpit shows the
    honest "not reporting / no audit yet" state, NEVER a fabricated all-clear. Best-effort: any
    read/parse hiccup returns ``None``.

    Existence is checked via ``get_blob_metadata`` FIRST (returns ``None`` for a missing object,
    like ``_index_absolutes``) so a missing object never surfaces the provider's raw ``NotFound``
    (404) — which is NOT an ``OSError`` — as a 5xx on this read-only endpoint.
    """
    try:
        if client.get_blob_metadata(bucket, blob) is None:
            return None  # object absent → not reporting
        raw = client.download_bytes(bucket, blob)
    except (OSError, ValueError, RuntimeError):
        return None
    try:
        parsed = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _read_latest_run(client: StorageClient, bucket: str) -> dict[str, object] | None:
    """The consolidator's self-published ``_index/latest.json`` run summary, or ``None`` if absent.

    Absent = this consolidator has never run under the summary-emitting code (dead / not-yet-fired /
    not-yet-redeployed) → the cockpit shows it as "not yet reporting", NEVER a fabricated all-clear.
    """
    return _read_index_json(client, bucket, _LATEST_RUN_BLOB)


class _AuditFields(TypedDict):
    """The audit-summary kwargs spread into ``ConsolidatorHealth`` — one typed slot per model field."""

    phantom_audit_at: str | None
    phantom_count: int | None
    phantom_triage_link: str | None
    reprobe_audit_at: str | None
    reprobe_new_empties: int | None
    reprobe_disagreements: int | None
    reprobe_reclassified: int | None


def _audit_fields(client: StorageClient, bucket: str, kind: str) -> _AuditFields:
    """The last phantom-audit + empty-re-probe summaries for this consolidator's bucket.

    Two cheap ``_index/*.json`` reads, gated to the kinds an audit actually targets so
    features/execution/flat consolidators pay nothing. Every field is ``None`` when the summary is
    absent (honest "no audit yet") — never a fabricated clean verdict. Best-effort throughout.
    """
    fields = _AuditFields(
        phantom_audit_at=None,
        phantom_count=None,
        phantom_triage_link=None,
        reprobe_audit_at=None,
        reprobe_new_empties=None,
        reprobe_disagreements=None,
        reprobe_reclassified=None,
    )
    if kind not in _AUDIT_BEARING_KINDS:
        return fields
    phantom = _read_index_json(client, bucket, _PHANTOM_AUDIT_BLOB)
    if phantom is not None:
        fields["phantom_audit_at"] = _as_str(phantom.get("generated_at"))
        fields["phantom_count"] = _as_int(phantom.get("phantom_count"))
        fields["phantom_triage_link"] = _as_str(phantom.get("triage_jsonl"))
    reprobe = _read_index_json(client, bucket, _REPROBE_AUDIT_BLOB)
    if reprobe is not None:
        fields["reprobe_audit_at"] = _as_str(reprobe.get("generated_at"))
        fields["reprobe_new_empties"] = _as_int(reprobe.get("new_empties"))
        fields["reprobe_disagreements"] = _as_int(reprobe.get("disagreements"))
        fields["reprobe_reclassified"] = _as_int(reprobe.get("reclassified"))
    return fields


def _as_str(v: object) -> str | None:
    return v if isinstance(v, str) else None


def _as_int(v: object) -> int | None:
    return v if isinstance(v, int) and not isinstance(v, bool) else None


def _as_float(v: object) -> float | None:
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def _index_absolutes(client: StorageClient, bucket: str) -> tuple[int | None, int | None]:
    """``(row_count, size_bytes)`` for a bucket's consolidated index. Size is one metadata call;
    rows come from a cheap ranged parquet-footer read (never downloads the whole index). Best-effort:
    a missing index or a transient read hiccup returns ``None`` for the affected field — an
    observability nicety layered on the freshness posture, not a correctness gate."""
    try:
        meta = client.get_blob_metadata(bucket, _INDEX_BLOB)
    except (OSError, ValueError, RuntimeError):
        return None, None
    if meta is None:
        return None, None
    size = int(meta.size or 0)
    if size <= 0:
        return None, None
    try:
        import pyarrow as pa  # noqa: imports-inside-functions — lazy heavy dep, only for the footer read
        import pyarrow.parquet as pq  # noqa: imports-inside-functions

        native = pa.PythonFile(_RangedIndexReader(client, bucket, _INDEX_BLOB, size), mode="r")
        rows = pq.ParquetFile(native).metadata.num_rows
        return int(rows), size
    except (OSError, ValueError, RuntimeError):
        return None, size
