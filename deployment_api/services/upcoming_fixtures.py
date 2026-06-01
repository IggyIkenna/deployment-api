"""Read upcoming sports fixtures from rolling-window GCS parquets.

Consumes ``sports_reference/by_date/day={D}/entity=fixtures/fixtures.parquet`` in
``instruments-store-sports-{project}``. Missing dates or read failures are
skipped per-shard (no cross-day raise) per shard-level failure isolation.
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, date, datetime, timedelta
from typing import TypedDict

import pandas as pd
from unified_trading_library import resolve_bucket_name

from deployment_api.utils.storage_client import get_storage_client
from deployment_api.utils.storage_facade import object_exists

logger = logging.getLogger(__name__)

_MAX_DAYS = 31

# In-process TTL cache for upcoming fixtures. Keyed on (days, league_id).
# The function does up to 14 transpacific GCS round-trips per call (HEAD + GET
# per day in a 7-day window); when local API runs in US and bucket is in
# asia-northeast1 that's 30-40s of wallclock. The Data Status panel re-mounts
# this query on every tab open, so without caching every navigation pays the
# full transpacific cost. 5-min TTL is appropriate because (a) the parquets
# being read are themselves only refreshed by hourly upstream backfills, and
# (b) the UI shows kickoff times where 5 min of staleness is invisible.
_FIXTURES_CACHE: dict[tuple[int, str | None], tuple[float, list[UpcomingFixture]]] = {}
_FIXTURES_CACHE_TTL_SEC: int = 300


class UpcomingFixture(TypedDict):  # CORRECT-LOCAL — API response shape, no cross-service consumer
    """One fixture row for the upcoming fixtures API.

    Serialized directly as JSON by FastAPI. ``kickoff_utc`` is an ISO-8601
    string with ``+00:00`` offset so the UI gets a stable format to parse.
    """

    fixture_id: str
    kickoff_utc: str
    league_id: str
    home_team_id: str
    away_team_id: str
    home_team_name: str
    away_team_name: str
    venue_id: str
    venue_name: str
    status: str
    round: str


def _sports_bucket() -> str:
    return resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")


def _read_fixtures_parquet(bucket: str, object_path: str) -> pd.DataFrame:
    """Load a parquet object via UCI ``StorageClient`` (no ``gs://`` literals)."""
    from io import BytesIO

    import pyarrow.parquet as pq

    client = get_storage_client()
    blob = client.download_bytes(bucket, object_path)
    table = pq.read_table(BytesIO(blob), columns=None)  # type: ignore[reportUnknownVariableType]
    to_pandas = getattr(table, "to_pandas", None)  # type: ignore[reportUnknownArgumentType]
    if not callable(to_pandas):
        raise RuntimeError("pyarrow table missing to_pandas()")
    df_obj: object = to_pandas()
    if not isinstance(df_obj, pd.DataFrame):
        raise RuntimeError("pyarrow returned non-DataFrame payload")
    return df_obj


def _pandas_scalar_missing(val: object) -> bool:
    if val is None:
        return True
    try:
        return bool(pd.isna(val))  # type: ignore[reportCallIssue,reportUnknownArgumentType]
    except (TypeError, ValueError):
        return False


def _norm_str(val: object, default: str = "") -> str:
    if _pandas_scalar_missing(val):
        return default
    s = str(val).strip()
    return s if s else default


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _parse_kickoff_from_timestamp(val: object) -> datetime | None:
    if isinstance(val, datetime):
        return _ensure_utc(val)
    to_pd = getattr(val, "to_pydatetime", None)
    if callable(to_pd):
        try:
            return _ensure_utc(to_pd())  # pyright: ignore[reportArgumentType]
        except (ValueError, OSError, TypeError, AttributeError):
            return None
    return None


def _parse_kickoff_from_iso_string(raw: str) -> datetime | None:
    iso = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(iso)
    except (ValueError, TypeError, OSError):
        return None
    return _ensure_utc(dt)


def _parse_kickoff(val: object) -> datetime | None:
    if _pandas_scalar_missing(val):
        return None
    ts = _parse_kickoff_from_timestamp(val)
    if ts is not None:
        return ts
    s = str(val).strip()
    if not s:
        return None
    return _parse_kickoff_from_iso_string(s)


def _row_to_fixture(rec: object) -> UpcomingFixture | None:
    if not isinstance(rec, dict):
        return None
    row: dict[str, object] = {str(k): v for k, v in rec.items()}  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]
    kid = _norm_str(row.get("fixture_id"))
    if not kid:
        return None
    ko = _parse_kickoff(row.get("kickoff_utc"))
    if ko is None:
        return None
    rnd = _norm_str(row.get("round"))
    if not rnd:
        rnd = _norm_str(row.get("round_name"))
    return UpcomingFixture(
        fixture_id=kid,
        kickoff_utc=ko.isoformat(),
        league_id=_norm_str(row.get("league_id")),
        home_team_id=_norm_str(row.get("home_team_id")),
        away_team_id=_norm_str(row.get("away_team_id")),
        home_team_name=_norm_str(row.get("home_team_name")),
        away_team_name=_norm_str(row.get("away_team_name")),
        venue_id=_norm_str(row.get("venue_id")),
        venue_name=_norm_str(row.get("venue_name")),
        status=_norm_str(row.get("status")),
        round=rnd,
    )


def _read_one_day_frame(bucket: str, d: date) -> pd.DataFrame | None:
    object_path = f"sports_reference/by_date/day={d.isoformat()}/entity=fixtures/fixtures.parquet"
    try:
        if not object_exists(bucket, object_path):
            return None
        df = _read_fixtures_parquet(bucket, object_path)
        return df if not df.empty else None
    except (OSError, ValueError, RuntimeError) as exc:
        logger.warning(
            "upcoming fixtures read failed for %s/%s: %s",
            bucket,
            object_path,
            exc,
        )
        return None


def _read_frames_for_window(
    bucket: str,
    *,
    start: date,
    inclusive_extra_days: int,
) -> list[pd.DataFrame]:
    # Per-day reads are independent transpacific GCS HEAD + GET round-trips
    # (~2-3s each from US to asia-northeast1). A 7-day window serial = 30-40s.
    # Threading across days collapses to one round-trip latency.
    from concurrent.futures import ThreadPoolExecutor

    days = [start + timedelta(days=offset) for offset in range(0, inclusive_extra_days + 1)]
    with ThreadPoolExecutor(max_workers=min(len(days), 16)) as ex:
        results = list(ex.map(lambda d: _read_one_day_frame(bucket, d), days))  # pyright: ignore[reportUnknownArgumentType,reportUnknownLambdaType]
    return [df for df in results if df is not None]


def _dedupe_preserve_order(sorted_fixtures: list[UpcomingFixture]) -> list[UpcomingFixture]:
    seen: set[str] = set()
    unique: list[UpcomingFixture] = []
    for fx in sorted_fixtures:
        fid = fx["fixture_id"]
        if fid in seen:
            continue
        seen.add(fid)
        unique.append(fx)
    return unique


def list_upcoming_fixtures(
    *,
    days: int = 7,
    league_id: str | None = None,
    project_id: str | None = None,
) -> list[UpcomingFixture]:
    """Concatenate fixture rows for UTC today through today+``days`` (inclusive).

    Missing parquet objects are skipped. Read/parse errors for one day do not
    abort other days.
    """
    if days < 1:
        days = 1
    if days > _MAX_DAYS:
        days = _MAX_DAYS
    league_filter = league_id.strip() if league_id and league_id.strip() else None

    cache_key = (days, league_filter)
    now = time.monotonic()
    cached = _FIXTURES_CACHE.get(cache_key)
    if cached is not None and (now - cached[0]) < _FIXTURES_CACHE_TTL_SEC:
        return cached[1]

    bucket = _sports_bucket()
    today = datetime.now(UTC).date()

    frames = _read_frames_for_window(bucket, start=today, inclusive_extra_days=days)
    if not frames:
        _FIXTURES_CACHE[cache_key] = (now, [])
        return []

    all_df = pd.concat(frames, ignore_index=True)
    if league_filter and "league_id" in all_df.columns:
        all_df = all_df[all_df["league_id"].astype(str) == league_filter]

    if "kickoff_utc" not in all_df.columns:
        _FIXTURES_CACHE[cache_key] = (now, [])
        return []

    parsed: list[UpcomingFixture] = []
    for rec in all_df.to_dict(orient="records"):
        fx = _row_to_fixture(rec)
        if fx is not None:
            parsed.append(fx)

    parsed.sort(key=lambda f: f["kickoff_utc"])
    result = _dedupe_preserve_order(parsed)
    _FIXTURES_CACHE[cache_key] = (now, result)
    return result
