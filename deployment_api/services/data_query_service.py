"""
Data query service for file listing and path operations.

Handles GCS bucket operations, file listing, venue filtering,
and instrument availability queries.
"""

import logging
from datetime import UTC, datetime, timedelta
from typing import ClassVar, cast

import pandas as pd
from unified_api_contracts.internal import MarketCategory
from unified_trading_library import AssetGroup, build_bucket, resolve_bucket_name

from deployment_api.services.data_status_drilldown import build_bucket_name as _drilldown_build_bucket_name
from deployment_api.settings import gcp_project_id as _pid
from deployment_api.utils.storage_facade import (
    ObjectInfo,
    list_objects,
    list_prefixes,
    object_exists,
)

logger = logging.getLogger(__name__)


class DataQueryService:
    """
    Query service for data operations.

    This service handles:
    - GCS bucket file listing
    - Path-based data queries
    - Venue filtering operations
    - Instrument availability checks
    """

    def __init__(self, project_id: str | None = None):
        """Initialize data query service."""
        self.project_id = project_id or _pid

    async def list_files_in_path(
        self,
        bucket_name: str,
        path: str = "",
        max_results: int = 100,
        show_dirs: bool = False,
    ) -> dict[str, object]:
        """
        List files in a specific GCS bucket path.

        Args:
            bucket_name: GCS bucket name
            path: Path within bucket (optional)
            max_results: Maximum number of results to return
            show_dirs: Whether to include directory-like prefixes

        Returns:
            Dictionary containing file listing results
        """
        try:
            logger.info("Listing files in %s/%s", bucket_name, path)

            files: list[dict[str, object]] = []
            directories: set[str] = set()
            truncated: bool = False

            # List objects in the path
            objects: list[ObjectInfo] = list_objects(bucket_name, path, max_results=max_results * 2)

            for obj in objects:
                obj_path: str = obj.name
                # Skip the path itself if it matches exactly
                if obj_path == path:
                    continue

                # Extract relative path from the prefix
                if path and not obj_path.startswith(path):
                    continue

                relative_path = obj_path[len(path) :] if path else obj_path
                relative_path = relative_path.lstrip("/")

                # Check if this looks like a directory
                if "/" in relative_path:
                    # Extract directory name
                    dir_name = relative_path.split("/")[0]
                    full_dir_path = f"{path}/{dir_name}".strip("/") if path else dir_name
                    directories.add(full_dir_path)
                else:
                    # It's a file
                    files.append(
                        {
                            "name": relative_path,
                            "full_path": obj_path,
                            "size": None,  # Would need GCS metadata call to get size
                            "type": "file",
                        }
                    )

            # Limit results
            if len(files) > max_results:
                files = files[:max_results]
                truncated = True

            result: dict[str, object] = {
                "bucket": bucket_name,
                "path": path,
                "files": files,
                "directories": [{"name": d, "type": "directory"} for d in sorted(directories)],
                "total_count": len(files) + len(directories),
                "truncated": truncated,
            }

            return result

        except (OSError, ValueError, RuntimeError) as e:
            logger.error("Error listing files in %s/%s: %s", bucket_name, path, e)
            return {"error": str(e)}

    async def get_venue_filters(self, service: str) -> dict[str, object]:
        """
        Get available venue filters for a service.

        Args:
            service: Service name to get venues for

        Returns:
            Dictionary with ``service`` and ``asset_groups`` map (per-group venue lists).
        """
        # Per-service asset_group scope (bucket resolved via drilldown canonical mapping)
        _all_cats = [cat.value.lower() for cat in MarketCategory]
        service_asset_groups: dict[str, list[str]] = {
            "market-tick-data-handler": _all_cats,
            "market-data-processing-service": _all_cats,
            "instruments-service": _all_cats,
            "features-equity-service": ["tradfi"],
            "features-derivatives-service": ["cefi"],
            "features-defi-service": ["defi"],
        }

        ag_list = service_asset_groups.get(service)
        if not ag_list:
            return {"error": f"Unknown service: {service}"}

        by_asset_group: dict[str, dict[str, object]] = {}
        venue_filters: dict[str, object] = {
            "service": service,
            "asset_groups": by_asset_group,
        }

        for ag in ag_list:
            try:
                bucket_name = _drilldown_build_bucket_name(service, ag)
                venues: list[str] = []

                # List prefixes to find venue directories
                # Assuming structure: bucket/venue/date/... or bucket/date/venue/...
                prefixes = list_prefixes(bucket_name, "")

                # Extract venue names from prefixes
                for prefix in prefixes[:50]:  # Limit to avoid huge responses
                    # Remove trailing slash
                    clean_prefix = prefix.rstrip("/")

                    # Extract potential venue name
                    # This is heuristic - actual structure may vary
                    parts = clean_prefix.split("/")
                    if parts:
                        venue_name = parts[0]
                        if venue_name and venue_name not in venues:
                            venues.append(venue_name)

                by_asset_group[ag] = {
                    "venues": sorted(venues),
                    "count": len(venues),
                }

            except (OSError, ValueError, RuntimeError) as e:
                logger.debug("Error getting venues for %s: %s", ag, e)
                by_asset_group[ag] = {
                    "error": str(e),
                    "venues": [],
                    "count": 0,
                }

        return venue_filters

    async def get_instruments_list(
        self,
        asset_group: str,
        venue: str | None = None,
        instrument_type: str | None = None,
        limit: int = 100,
    ) -> dict[str, object]:
        """
        Get list of instruments for an asset group.

        Args:
            asset_group: Asset group (cefi, tradfi, defi)
            venue: Optional venue filter
            instrument_type: Optional instrument type filter
            limit: Maximum number of instruments to return

        Returns:
            Dictionary containing instruments list
        """
        try:
            # Map to instruments bucket
            bucket_name = resolve_bucket_name(
                cloud="gcp", kind="instruments-store", asset_group=cast(AssetGroup, asset_group)
            )

            # Build path based on filters
            path = ""
            if venue:
                path = f"{venue}/"
            if instrument_type:
                # Map instrument type to folder structure
                type_mapping = {
                    "SPOT": "spot_pairs",
                    "PERPETUAL": "perps",
                    "FUTURE": "futures",
                    "OPTION": "options",
                    "EQUITY": "equities",
                    "BOND": "bonds",
                    "ETF": "etfs",
                }
                folder = type_mapping.get(instrument_type.upper())
                if folder:
                    path += f"{folder}/"

            # List files to find instruments
            objects_list: list[ObjectInfo] = list_objects(bucket_name, path, max_results=limit * 2)

            instruments: list[str] = []
            truncated_instr: bool = False
            for obj in objects_list:
                # Extract instrument name from path
                # Assuming structure like: venue/type/instrument_name.parquet
                parts = obj.name.split("/")
                if len(parts) >= 2:
                    filename = parts[-1]
                    # Remove file extension
                    instrument_name = filename.rsplit(".", 1)[0] if "." in filename else filename

                    if instrument_name and instrument_name not in instruments:
                        instruments.append(instrument_name)

                        if len(instruments) >= limit:
                            truncated_instr = True
                            break

            instruments_result: dict[str, object] = {
                "asset_group": asset_group,
                "venue": venue,
                "instrument_type": instrument_type,
                "instruments": sorted(instruments),
                "total_count": len(instruments),
                "truncated": truncated_instr,
            }

            return instruments_result

        except (OSError, ValueError, RuntimeError) as e:
            logger.error("Error getting instruments list: %s", e)
            return {"error": str(e)}

    # Categories the search walks when no specific category is requested. Order
    # matters for deterministic test output — keep alphabetical except SPORTS
    # last (its registry is the largest, most-cached).
    _SEARCH_CATEGORIES: tuple[str, ...] = ("cefi", "defi", "prediction", "tradfi", "sports")

    # Conservative cap on per-category enumeration — production buckets carry
    # thousands of instruments, but a search is interactive (user typing) so we
    # only need a wide-enough net to find good matches. Truncation surfaces in
    # the response so the UI can warn.
    _SEARCH_LISTING_CAP: int = 2000

    async def search_instruments(
        self,
        query: str,
        asset_group: str | None = None,
        limit: int = 50,
    ) -> dict[str, object]:
        """Case-insensitive substring search for canonical instrument IDs.

        Walks one or all category-specific instruments buckets and returns
        every (canonical_id, category, venue, instrument_type) tuple whose
        canonical_id contains ``query`` (case-insensitive). The ``query`` is
        also tokenised on whitespace — every token must be present in the
        canonical_id (AND-match) so users can type ``usdc weth 500`` to find
        a UNISWAP_V3 USDC-WETH-500 pool without knowing the canonical ordering.

        Args:
            query: Search query (case-insensitive substring; whitespace =
                AND-match across tokens). Empty query returns ``[]`` — we
                don't dump the entire registry by default to avoid surprising
                users with thousands of rows.
            asset_group: Single asset group to search. ``None`` (the institutional
                cross-asset-group default) walks all five canonical groups.
            limit: Max matches returned. Truncation flag in response.

        Returns:
            ``{
                query: str,
                asset_group: str | None,
                matches: [
                    {canonical_id, asset_group, venue, instrument_type}
                ],
                total_matches: int,
                truncated: bool,
                # Debug — counts per asset group that the search actually walked,
                # useful for diagnosing "why am I not getting matches"
                asset_groups_searched: list[str],
            }``
        """
        query_normalised = (query or "").strip()
        if not query_normalised:
            return {
                "query": "",
                "asset_group": asset_group,
                "matches": [],
                "total_matches": 0,
                "truncated": False,
                "asset_groups_searched": [],
            }

        query_tokens: list[str] = [t.lower() for t in query_normalised.split() if t.strip()]

        # Resolve category list to walk.
        cats_to_walk: list[str] = [asset_group.lower()] if asset_group else list(self._SEARCH_CATEGORIES)

        all_matches: list[dict[str, str]] = []
        truncated = False
        for cat in cats_to_walk:
            cat_matches = await self._search_in_category(cat, query_tokens, limit)
            all_matches.extend(cat_matches)
            if len(all_matches) >= limit:
                truncated = True
                all_matches = all_matches[:limit]
                break

        # Deduplicate on (canonical_id, category, venue, instrument_type) tuple.
        seen: set[tuple[str, str, str, str]] = set()
        deduped: list[dict[str, str]] = []
        for m in all_matches:
            key = (m["canonical_id"], m["asset_group"], m["venue"], m["instrument_type"])
            if key in seen:
                continue
            seen.add(key)
            deduped.append(m)

        # Sort by canonical_id for deterministic ordering — the UI dropdown
        # benefits from stable ordering across keystrokes (no result-shuffle).
        deduped.sort(key=lambda m: (m["canonical_id"], m["venue"]))

        return {
            "query": query_normalised,
            "asset_group": asset_group,
            "matches": deduped,
            "total_matches": len(deduped),
            "truncated": truncated,
            "asset_groups_searched": cats_to_walk,
        }

    async def _search_in_category(
        self,
        category: str,
        query_tokens: list[str],
        limit: int,
    ) -> list[dict[str, str]]:
        """Walk one category's canonical-ID corpus, return matches.

        Sources of truth differ per category:

        - **SPORTS**: ``_index/availability_index.parquet`` has the canonical
          ``league_id`` column populated (EPL, BUNDESLIGA, …). One small
          parquet read; tens of thousands of rows; very fast.
        - **CEFI / TRADFI / DEFI / PREDICTION**: the index doesn't carry
          ``instrument_id`` per-row; the canonical ``instrument_key`` lives
          inside per-venue ``instruments.parquet`` files written daily under
          ``instrument_availability/by_date/day=.../venue=.../``. We pick the
          most-recent day with data and scan the per-venue parquets in parallel.

        Both paths cache the loaded canonical-ID corpus in-process for 5
        minutes so successive search keystrokes don't re-hit GCS.
        """
        category = category.lower()
        try:
            corpus = self._load_search_corpus(category)
        except (OSError, ValueError, RuntimeError) as exc:
            logger.warning(
                "search_instruments: failed to load %s corpus — %s: %s",
                category,
                type(exc).__name__,
                exc,
            )
            return []

        matches: list[dict[str, str]] = []
        for row in corpus:
            cid_lower = row["canonical_id"].lower()
            if not all(t in cid_lower for t in query_tokens):
                continue
            matches.append({**row, "asset_group": category.upper()})
            if len(matches) >= limit:
                break
        return matches

    # In-process corpus cache: ``{category: (loaded_at_epoch, [{canonical_id,
    # venue, instrument_type}, ...])}``. 5-minute TTL — searches are interactive
    # but the canonical-ID corpus changes once daily at most.
    _CORPUS_TTL_SECONDS: ClassVar[int] = 300
    _corpus_cache: ClassVar[dict[str, tuple[float, list[dict[str, str]]]]] = {}

    def _load_search_corpus(self, category: str) -> list[dict[str, str]]:
        """Return the cached canonical-ID corpus for ``category``.

        Cache miss / stale → reload from GCS. The corpus is a list of dicts
        ``{canonical_id, venue, instrument_type}`` — the search filter applies
        token-AND substring matching on top.
        """
        import time

        now = time.monotonic()
        cached = self._corpus_cache.get(category)
        if cached is not None and (now - cached[0]) < self._CORPUS_TTL_SECONDS:
            return cached[1]

        if category == "sports":
            corpus = self._load_sports_corpus_from_index()
        else:
            corpus = self._load_corpus_from_per_venue_parquets(category)

        self._corpus_cache[category] = (now, corpus)
        return corpus

    def _load_sports_corpus_from_index(self) -> list[dict[str, str]]:
        """Read ``instruments-store-sports/_index/availability_index.parquet``.

        Sports' canonical ID is the league_id (EPL, BUNDESLIGA, ...) - the
        index has it populated for every league x venue x data_type tuple. We
        deduplicate to ``(league_id, venue, instrument_type)`` for the search
        return, treating ``league_id`` as the ``canonical_id``.
        """
        _sports_bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
        gs_uri = f"gs://{_sports_bucket}/_index/availability_index.parquet"  # noqa: gs-uri  — URI composer, bucket resolved via resolve_bucket_name
        df = self._read_parquet_columns_safe(gs_uri, ["league_id", "venue", "instrument_type"])
        if df is None or df.empty:
            return []
        # Empty league_id means a row that wasn't sports-canonical — skip.
        df = df[df["league_id"].astype(str).str.len() > 0]  # type: ignore[reportUnknownVariableType, reportUnknownMemberType]
        unique = df.drop_duplicates(subset=["league_id", "venue", "instrument_type"])  # type: ignore[reportUnknownVariableType, reportUnknownMemberType]
        corpus: list[dict[str, str]] = []
        for _, row in unique.iterrows():  # type: ignore[reportUnknownVariableType, reportUnknownMemberType]
            corpus.append(
                {
                    "canonical_id": str(row["league_id"]),  # type: ignore[reportUnknownArgumentType]
                    "venue": str(row["venue"] or ""),  # type: ignore[reportUnknownArgumentType]
                    "instrument_type": str(row["instrument_type"] or ""),  # type: ignore[reportUnknownArgumentType]
                }
            )
        return corpus

    def _load_corpus_from_per_venue_parquets(self, category: str) -> list[dict[str, str]]:
        """Scan the latest day's per-venue ``instruments.parquet`` files.

        For CeFi / TradFi / DeFi / Prediction the canonical ``instrument_key``
        lives inside per-venue parquets written under
        ``instrument_availability/by_date/day=.../venue=.../instruments.parquet``.

        Strategy: list venues for the most recent day with data, read each
        venue's parquet, extract ``instrument_key`` + ``instrument_type``,
        return the union. Bounded by ``_SEARCH_LISTING_CAP`` parquet reads.
        """
        bucket = build_bucket("instruments", project_id=self.project_id, asset_group=category)
        latest_day = self._latest_available_day(bucket)
        if latest_day is None:
            return []
        per_venue_uris = self._collect_per_venue_uris(bucket, latest_day)
        corpus: list[dict[str, str]] = []
        seen: set[tuple[str, str, str]] = set()
        for venue, uri in per_venue_uris.items():
            self._extend_corpus_from_venue_parquet(uri, venue, corpus, seen)
        return corpus

    def _collect_per_venue_uris(self, bucket: str, day: str) -> dict[str, str]:
        """List the day's blobs and extract one ``instruments.parquet`` per venue."""
        prefix = f"instrument_availability/by_date/day={day}/"
        try:
            objects = list_objects(bucket, prefix, max_results=self._SEARCH_LISTING_CAP)
        except (OSError, ValueError, RuntimeError) as exc:
            logger.warning("search_instruments: list failed for %s/%s — %s", bucket, prefix, exc)
            return {}
        per_venue: dict[str, str] = {}
        for obj in objects:
            if not obj.name.endswith("/instruments.parquet"):
                continue
            venue = ""
            for p in obj.name.split("/"):
                if p.startswith("venue="):
                    venue = p[len("venue=") :]
                    break
            if venue:
                per_venue[venue] = f"gs://{bucket}/{obj.name}"  # noqa: gs-uri  — URI composer, bucket already resolved
        return per_venue

    def _extend_corpus_from_venue_parquet(
        self,
        uri: str,
        venue: str,
        corpus: list[dict[str, str]],
        seen: set[tuple[str, str, str]],
    ) -> None:
        """Read one venue's instruments.parquet and append unique rows to corpus."""
        df = self._read_parquet_columns_safe(uri, ["instrument_key", "instrument_type"])
        if df is None or df.empty:
            return
        for _, row in df.iterrows():
            cid = str(row.get("instrument_key") or "").strip()
            if not cid:
                continue
            it = str(row.get("instrument_type") or "").strip()
            key = (cid, venue, it)
            if key in seen:
                continue
            seen.add(key)
            corpus.append({"canonical_id": cid, "venue": venue, "instrument_type": it})

    def _latest_available_day(self, bucket: str) -> str | None:
        """Find the most-recent ``day=YYYY-MM-DD`` partition in the bucket's
        ``instrument_availability/by_date/`` index.

        Note: production ``list_prefixes`` has a delimiter-handling bug for
        direct-GCS (non-FUSE) mode — it iterates over blobs only, missing the
        common-prefix sentinels GCS returns when ``delimiter="/"`` is set. We
        work around by walking ``list_objects`` (which sees real blobs only)
        and extracting the ``day=`` partition labels from their full paths.
        Bounded by ``_SEARCH_LISTING_CAP`` so a busy bucket doesn't blow up.
        """
        try:
            objects = list_objects(
                bucket,
                "instrument_availability/by_date/",
                max_results=self._SEARCH_LISTING_CAP,
            )
        except (OSError, ValueError, RuntimeError) as exc:
            logger.warning("search_instruments: by_date list failed for %s — %s", bucket, exc)
            return None
        days: set[str] = set()
        for obj in objects:
            for token in obj.name.split("/"):
                if token.startswith("day="):
                    days.add(token[len("day=") :])
                    break
        if not days:
            return None
        # ISO YYYY-MM-DD sorts lexicographically.
        return max(days)

    @staticmethod
    def _read_parquet_columns_safe(gs_uri: str, columns: list[str]) -> pd.DataFrame | None:
        """Read ``columns`` from a parquet at ``gs_uri``; return DataFrame or None.

        Uses pyarrow + gcsfs locally. Failures (network, schema mismatch,
        missing column) return None so the caller can fall back gracefully.
        """
        import gcsfs  # pyright: ignore[reportMissingModuleSource]
        import pyarrow.parquet as pq  # pyright: ignore[reportMissingModuleSource]

        if not gs_uri.startswith("gs://"):  # noqa: gs-uri (parsing a caller-supplied URI, not constructing one)
            return None
        bucket_key = gs_uri[len("gs://") :]  # noqa: gs-uri (parsing a caller-supplied URI, not constructing one)
        try:
            fs = gcsfs.GCSFileSystem()
            with fs.open(bucket_key, "rb") as fh:  # type: ignore[reportUnknownMemberType]
                pf = pq.ParquetFile(fh)
                schema_names = set(pf.schema_arrow.names)  # type: ignore[reportUnknownVariableType, reportUnknownMemberType, reportUnknownArgumentType]
                # Only request columns that actually exist (graceful drift handling).
                proj = [c for c in columns if c in schema_names]
                if not proj:
                    return None
                table = pf.read(columns=proj)  # type: ignore[reportUnknownVariableType, reportUnknownMemberType]
                return table.to_pandas()  # type: ignore[reportUnknownMemberType, reportUnknownVariableType]
        except (OSError, ValueError, RuntimeError) as exc:
            logger.warning("search_instruments: parquet read failed for %s — %s", gs_uri, exc)
            return None

    @staticmethod
    def _extract_canonical_id(path: str) -> tuple[list[str], str] | None:
        """Return ``(parts, canonical_id)`` for a parquet path; None for sentinels."""
        parts = path.split("/")
        if not parts or parts[0].startswith("_"):
            return None
        filename = parts[-1]
        if not filename or "." not in filename:
            return None
        canonical_id = filename.rsplit(".", 1)[0]
        if not canonical_id:
            return None
        return parts, canonical_id

    @staticmethod
    def _parse_partitioned_layout(parts: list[str]) -> tuple[str, str]:
        """Walk ``key=value`` partition labels in by-date GCS paths.

        Returns ``(venue, instrument_type)`` — ``instrument_type`` falls back
        to the ``entity=`` partition (sports / prediction availability layout)
        when no explicit ``instrument_type=`` partition exists.
        """
        venue = ""
        instrument_type = ""
        for p in parts:
            if p.startswith("venue="):
                venue = p[len("venue=") :]
            elif p.startswith("instrument_type="):
                instrument_type = p[len("instrument_type=") :]
            elif p.startswith("entity=") and not instrument_type:
                instrument_type = p[len("entity=") :]
        return venue, instrument_type

    @classmethod
    def _parse_instrument_object_path(cls, path: str) -> tuple[str, str, str] | None:
        """Parse a GCS object path into ``(canonical_id, venue, instrument_type)``.

        Handles two known layouts:

        - **Per-venue layout** (CeFi/TradFi/DeFi):
          ``{venue}/{instrument_type_folder}/{canonical_id}.parquet``
        - **By-date layout** (sports/prediction availability):
          ``instrument_availability/by_date/day=.../venue=.../{canonical_id}.parquet``
          and partitioned ``by_date/day=.../entity={entity}/{canonical_id}.parquet``

        Returns ``None`` for paths that don't fit either pattern (sentinel files,
        ``_index/``, ``_vm_staging/``, etc.).
        """
        extracted = cls._extract_canonical_id(path)
        if extracted is None:
            return None
        parts, canonical_id = extracted
        # Per-venue layout: venue/type/file.parquet (3 parts minimum)
        if len(parts) >= 3 and not parts[0].startswith("instrument_availability"):
            return canonical_id, parts[0], parts[1]
        # By-date layout: walk ``key=value`` partition labels
        venue, instrument_type = cls._parse_partitioned_layout(parts[:-1])
        if not venue and not instrument_type:
            return None
        return canonical_id, venue, instrument_type

    def _venue_to_category(self, venue: str) -> str | None:
        """Map a venue name to its market category (CEFI/TRADFI/DEFI), or None."""
        venue_upper = venue.upper()
        if any(k in venue_upper for k in ["BINANCE", "BYBIT", "OKX", "DERIBIT"]):
            return "CEFI"
        if any(k in venue_upper for k in ["NYSE", "NASDAQ", "CME", "CBOE", "ICE"]):
            return "TRADFI"
        if any(k in venue_upper for k in ["UNISWAP", "AAVE", "CURVE", "BALANCER"]):
            return "DEFI"
        return None

    def _parse_avail_date(self, raw: str, label: str) -> datetime | None:
        """Parse an availability date string to a timezone-aware datetime."""
        try:
            if "T" in raw:
                return datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return datetime.strptime(raw, "%Y-%m-%d").replace(tzinfo=UTC)
        except (ValueError, TypeError):
            logger.warning("Could not parse %s: %s", label, raw)
            return None

    def _default_data_types(self, category: str) -> list[str]:
        """Return default data types for a given market category."""
        defaults: dict[str, list[str]] = {
            "CEFI": ["trades", "book_snapshot_5"],
            "TRADFI": ["trades", "ohlcv_1m", "tbbo"],
            "DEFI": ["dex_pool_swaps", "lending_indices"],
        }
        return defaults.get(category, ["trades"])

    def _check_daily_availability(
        self,
        bucket_name: str,
        venue: str,
        instrument_type: str,
        instrument: str,
        data_types: list[str],
        effective_start: datetime,
        effective_end: datetime,
    ) -> dict[str, dict[str, bool]]:
        """Check data existence for each day in the effective range."""
        daily: dict[str, dict[str, bool]] = {}
        current_dt = effective_start
        while current_dt <= effective_end:
            date_str = current_dt.strftime("%Y-%m-%d")
            daily[date_str] = {}
            for dt in data_types:
                path = f"{venue}/{instrument_type.lower()}/{instrument}/{date_str}/{dt}"
                daily[date_str][dt] = object_exists(bucket_name, path)
            current_dt += timedelta(days=1)
        return daily

    async def get_instrument_availability(
        self,
        venue: str,
        instrument_type: str,
        instrument: str,
        start_date: str,
        end_date: str,
        data_type: str | None = None,
        available_from: str | None = None,
        available_to: str | None = None,
    ) -> dict[str, object]:
        """
        Check instrument availability over a date range.

        Returns dictionary containing availability analysis.
        """
        try:
            asset_group = self._venue_to_category(venue)
            if not asset_group:
                return {"error": f"Could not determine asset group for venue: {venue}"}

            bucket_name = resolve_bucket_name(
                cloud="gcp", kind="market-data", asset_group=cast(AssetGroup, asset_group.lower())
            )

            try:
                start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=UTC)
                end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=UTC)
            except ValueError as e:
                return {"error": f"Invalid date format: {e}"}

            avail_from = self._parse_avail_date(available_from, "available_from") if available_from else None
            avail_to = self._parse_avail_date(available_to, "available_to") if available_to else None

            effective_start = max(start_dt, avail_from) if avail_from else start_dt
            effective_end = min(end_dt, avail_to) if avail_to else end_dt

            data_types = [data_type] if data_type else self._default_data_types(asset_group)

            daily_availability = self._check_daily_availability(
                bucket_name,
                venue,
                instrument_type,
                instrument,
                data_types,
                effective_start,
                effective_end,
            )

            total_days = len(daily_availability)
            available_days = sum(1 for d in daily_availability.values() if any(d.values()))

            return {
                "venue": venue,
                "instrument_type": instrument_type,
                "instrument": instrument,
                "date_range": {"start": start_date, "end": end_date},
                "effective_range": {
                    "start": effective_start.strftime("%Y-%m-%d"),
                    "end": effective_end.strftime("%Y-%m-%d"),
                },
                "data_types": data_types,
                "daily_availability": daily_availability,
                "summary": {
                    "total_days": total_days,
                    "available_days": available_days,
                    "missing_days": total_days - available_days,
                    "availability_rate": (available_days / total_days * 100 if total_days > 0 else 0.0),
                },
            }

        except (OSError, ValueError, RuntimeError) as e:
            logger.error("Error checking instrument availability: %s", e)
            return {"error": str(e)}
