"""
Storage facade for GCS access with optional GCS FUSE support.

When DEPLOYMENT_ENV=production and GCS buckets are mounted via Cloud Run volume
mounts at /mnt/gcs/{bucket_name}, uses filesystem operations (faster, cached).
Otherwise (development, or FUSE not mounted) falls back to GCS API.

Toggle: Only use FUSE when DEPLOYMENT_ENV=production (injected at deployment).
Local dev uses DEPLOYMENT_ENV=development and always goes through GCS API.
"""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from typing import cast

from deployment_api.settings import DEPLOYMENT_ENV, STATE_BUCKET

logger = logging.getLogger(__name__)


def get_gcs_fuse_status() -> dict:
    """Return API's current GCS Fuse status for UI display."""
    env_ok = DEPLOYMENT_ENV == "production"
    mounted = bool(env_ok and STATE_BUCKET and _is_bucket_mounted(STATE_BUCKET))
    return {
        "active": mounted,
        "env": DEPLOYMENT_ENV,
        "reason": (
            "production + mount exists"
            if mounted
            else "development (fallback)"
            if not env_ok
            else "production but mount missing (fallback)"
        ),
    }


# Base path for GCS FUSE mounts (Cloud Run: each bucket at /mnt/gcs/{bucket_name})
GCS_FUSE_MOUNT_BASE = "/mnt/gcs"


def _use_gcs_fuse() -> bool:
    """Only use FUSE when DEPLOYMENT_ENV=production."""
    return DEPLOYMENT_ENV == "production"


def _fuse_path(bucket_name: str, object_path: str = "") -> Path:
    """Build FUSE path: /mnt/gcs/{bucket}/{path}."""
    base = Path(GCS_FUSE_MOUNT_BASE) / bucket_name
    if object_path:
        return base / object_path.lstrip("/")
    return base


def _is_bucket_mounted(bucket_name: str) -> bool:
    """Check if bucket is mounted at expected FUSE path."""
    path = _fuse_path(bucket_name)
    return path.exists() and path.is_dir()


@dataclass
class ObjectInfo:
    """Unified object metadata (from FUSE or GCS API). Blob-compatible interface."""

    name: str
    updated: datetime | None = None
    size: int | None = None
    time_created: datetime | None = None  # GCS has this; FUSE uses updated as fallback

    def __post_init__(self) -> None:
        if self.time_created is None and self.updated is not None:
            self.time_created = self.updated


def list_objects(
    bucket_name: str,
    prefix: str,
    max_results: int | None = None,
    delimiter: str | None = None,
) -> list[ObjectInfo]:
    """
    List objects at prefix. Uses FUSE when production + mounted, else GCS API.

    Returns list of ObjectInfo with name (full path), updated, size.
    """
    if _use_gcs_fuse() and _is_bucket_mounted(bucket_name):
        return _list_objects_fuse(bucket_name, prefix, max_results, delimiter)
    return _list_objects_api(bucket_name, prefix, max_results, delimiter)


def _list_objects_fuse(  # noqa: C901
    bucket_name: str,
    prefix: str,
    max_results: int | None,
    delimiter: str | None,
) -> list[ObjectInfo]:
    """List via FUSE filesystem. Matches GCS list_blobs(prefix) semantics - all objects under prefix."""  # noqa: E501
    try:
        base = _fuse_path(bucket_name)
        path = _fuse_path(bucket_name, prefix)
        if not path.exists():
            return []
        if path.is_file():
            rel = str(path.relative_to(base)).replace("\\", "/")
            st = path.stat()
            return [
                ObjectInfo(
                    name=rel,
                    updated=(datetime.fromtimestamp(st.st_mtime, tz=UTC) if st.st_mtime else None),
                    size=st.st_size if st.st_size else None,
                )
            ]

        results: list[ObjectInfo] = []
        # rglob("*") gets all files under prefix (matches GCS list_blobs semantics)
        for p in sorted(path.rglob("*")):
            if max_results and len(results) >= max_results:
                break
            if not p.is_file():
                continue
            try:
                rel = str(p.relative_to(base)).replace("\\", "/")
            except ValueError as e:
                logger.debug("Skipping item due to %s: %s", type(e).__name__, e)
                continue
            st = p.stat()
            results.append(
                ObjectInfo(
                    name=rel,
                    updated=(datetime.fromtimestamp(st.st_mtime, tz=UTC) if st.st_mtime else None),
                    size=st.st_size if st.st_size else None,
                )
            )
        return results
    except (OSError, ValueError, RuntimeError) as e:
        logger.debug("FUSE list failed for %s/%s: %s, falling back", bucket_name, prefix, e)
        return _list_objects_api(bucket_name, prefix, max_results, delimiter)


def _list_objects_api(
    bucket_name: str,
    prefix: str,
    max_results: int | None,
    delimiter: str | None,
) -> list[ObjectInfo]:
    """List via GCS API."""
    from deployment_api.utils.storage_client import get_storage_client

    client = get_storage_client()
    bucket = client.bucket(bucket_name)
    kwargs: dict = {"prefix": prefix}
    if max_results:
        kwargs["max_results"] = max_results
    if delimiter:
        kwargs["delimiter"] = delimiter
    blobs = list(bucket.list_blobs(**kwargs))
    return [
        ObjectInfo(
            name=b.name,
            updated=b.updated,
            size=b.size,
            time_created=cast(datetime | None, getattr(b, "time_created", b.updated)),
        )
        for b in blobs
    ]


def object_exists(bucket_name: str, object_path: str) -> bool:
    """
    Check if object exists. Uses FUSE when production + mounted, else GCS API.
    """
    if _use_gcs_fuse() and _is_bucket_mounted(bucket_name):
        try:
            path = _fuse_path(bucket_name, object_path)
            return path.exists() and path.is_file()
        except (OSError, ValueError, RuntimeError) as e:
            logger.debug("FUSE exists failed for %s/%s: %s", bucket_name, object_path, e)
            pass
    # Fallback to API
    from deployment_api.utils.storage_client import get_storage_client

    client = get_storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_path)
    return blob.exists()


def list_prefixes(
    bucket_name: str,
    prefix: str,
) -> list[str]:
    """
    List immediate child prefixes (GCS delimiter="/" semantics).
    Returns list of prefix strings like "prefix/child_folder/".
    """
    if _use_gcs_fuse() and _is_bucket_mounted(bucket_name):
        try:
            path = _fuse_path(bucket_name, prefix)
            if not path.exists() or not path.is_dir():
                return []
            return [f"{prefix.rstrip('/')}/{p.name}/" for p in path.iterdir() if p.is_dir()]
        except (OSError, ValueError, RuntimeError) as e:
            logger.debug("FUSE list_prefixes failed for %s/%s: %s", bucket_name, prefix, e)
            pass
    from deployment_api.utils.storage_client import get_storage_client

    client = get_storage_client()
    bucket = client.bucket(bucket_name)
    it = bucket.list_blobs(prefix=prefix, delimiter="/")
    list(it)  # Consume to populate .prefixes
    return list(getattr(it, "prefixes", []) or [])


def list_blobs_compat(
    bucket_name: str,
    prefix: str,
    max_results: int | None = None,
    delimiter: str | None = None,
):
    """
    Compatibility layer: returns iterator of blob-like objects with .name, .updated, .size.

    Use when callers expect blob attributes. Yields ObjectInfo (has same interface).
    """
    objs = list_objects(bucket_name, prefix, max_results, delimiter)
    yield from objs


def read_object_bytes(bucket_name: str, object_path: str) -> bytes:
    """
    Read object content as bytes. Uses FUSE when production + mounted, else GCS API.
    """
    if _use_gcs_fuse() and _is_bucket_mounted(bucket_name):
        try:
            path = _fuse_path(bucket_name, object_path)
            if path.exists() and path.is_file():
                return path.read_bytes()
        except (OSError, ValueError, KeyError) as e:
            logger.debug("FUSE read failed, falling back to GCS API: %s", e)
    from deployment_api.utils.storage_client import get_storage_client

    client = get_storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_path)
    return blob.download_as_bytes()


def read_object_text(bucket_name: str, object_path: str) -> str:
    """
    Read object content as text. Uses FUSE when production + mounted, else GCS API.
    """
    return read_object_bytes(bucket_name, object_path).decode("utf-8")


def write_object_bytes(bucket_name: str, object_path: str, data: bytes) -> None:
    """
    Write bytes to object. Uses FUSE when production + mounted (writable), else GCS API.
    """
    if _use_gcs_fuse() and _is_bucket_mounted(bucket_name):
        try:
            path = _fuse_path(bucket_name, object_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            return
        except (OSError, ValueError, RuntimeError) as e:
            logger.debug("FUSE write failed, falling back to GCS API: %s", e)
    from deployment_api.utils.storage_client import get_storage_client

    client = get_storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_path)
    blob.upload_from_file(BytesIO(data), rewind=True)


def write_object_text(bucket_name: str, object_path: str, data: str) -> None:
    """
    Write text to object. Uses FUSE when production + mounted, else GCS API.
    """
    write_object_bytes(bucket_name, object_path, data.encode("utf-8"))


def delete_object(bucket_name: str, object_path: str) -> None:
    """
    Delete object. Always uses GCS API (FUSE delete not always supported).
    """
    from deployment_api.utils.storage_client import get_storage_client

    client = get_storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_path)
    if blob.exists():
        blob.delete()


def get_storage_client_and_bucket(bucket_name: str):
    """Get GCS client and bucket for operations that need direct API (download, upload)."""
    from deployment_api.utils.storage_client import get_storage_client

    return get_storage_client(), get_storage_client().bucket(bucket_name)
