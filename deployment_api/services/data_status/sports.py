"""Sports reference-venue classification + reference expected dates.

Split out of the 6,663-line ``data_status_service.py`` god-module
(codex ratchet plan 2026-06-10). The facade module re-exports every
public + legacy-underscore name, so callers keep importing from
``deployment_api.services.data_status_service``.
"""

import logging
import time
from typing import ClassVar

import deployment_api.services.data_status_service as _dss
from deployment_api.services.data_status_drilldown import (
    COMMODITY_BUCKET_TEMPLATE,
    PREDICTION_KIND_MAP,
    SERVICE_TO_KIND,
)

logger = logging.getLogger(__name__)

from deployment_api.services.data_status.defi import DefiStatusMixin


class SportsStatusMixin(DefiStatusMixin):
    """Sports reference venues, sparse entities + reference dates.

    The data_status mixins form a single linear inheritance chain
    (cli -> defi -> sports -> breakdowns_domain -> breakdowns_core ->
    venue_resolution -> coverage -> missing_shards -> manifest) so that
    every cross-group ``self._method`` reference resolves statically
    under basedpyright strict. ``DataStatusService`` composes the top of
    the chain and is the ONLY public entry point — import it from
    ``deployment_api.services.data_status_service`` (the facade).
    """

    # Sports reference venues — fixture-dependent, not every-calendar-day expected.
    # Expected dates = dates where ANY sports reference entity has data (fixture calendar).
    _SPORTS_REFERENCE_PREFIXES: ClassVar[tuple[str, ...]] = (
        "FOOTYSTATS_",
        "UNDERSTAT_",
        "SFI_",
        "API_FOOTBALL_",
    )

    # Understat XG covers only 6 leagues — denominator must be filtered to
    # fixture dates from these leagues only, not the full fixture calendar.
    # Mapping: Understat league name → canonical league_id in LEAGUE_REGISTRY.
    _UNDERSTAT_LEAGUE_IDS: ClassVar[tuple[str, ...]] = (
        "EPL",
        "LA_LIGA",
        "BUNDESLIGA",
        "SERIE_A",
        "LIGUE_1",
        # RFPL (Russian) — not in LEAGUE_REGISTRY; omitted from calendar filter.
    )

    # Transfermarkt is fixture-INDEPENDENT reference data (squad composition,
    # league metadata). It's fetched on every batch day, not just fixture days.
    # Player values are expected year-round; transfer_records (future) only
    # during/after transfer windows.
    _TRANSFER_WINDOW_PREFIXES: ClassVar[tuple[str, ...]] = ("TRANSFERMARKT_",)

    def _is_sports_reference_venue(self, venue: str) -> bool:
        """Check if a venue is a sports reference entity (fixture-dependent)."""
        return venue.startswith(self._SPORTS_REFERENCE_PREFIXES)

    # Sparse sports entities where the full fixture calendar is the wrong
    # denominator. These are either provider-specific (Understat = 6 leagues),
    # infrequent (Transfermarkt = transfer windows), or partially backfilled
    # (FootyStats predictions/matches). For these, expected = found (show raw
    # count, not misleading percentage against 2660 fixture dates).
    _SPARSE_SPORTS_ENTITIES: ClassVar[frozenset[str]] = frozenset(
        {
            "XG",
            "UNDERSTAT_XG",  # Understat: 6 leagues only
            "PLAYER_VALUES",  # Transfermarkt: window-based
            "MATCHES",
            "PREDICTIONS",  # FootyStats: partial backfill
            # TRANSFERMARKT_LEAGUES + SFI_LEAGUES retired 2026-05-05
        }
    )

    def _is_understat_venue(self, venue: str) -> bool:
        """Check if a venue is Understat XG (6-league subset)."""
        v = venue.upper()
        return v == "XG" or v == "UNDERSTAT_XG" or v.startswith("UNDERSTAT_")

    def _is_transfer_window_venue(self, venue: str) -> bool:
        """Check if a venue is transfer-window-aware (Transfermarkt)."""
        v = venue.upper()
        return v.startswith("TRANSFERMARKT") or v == "PLAYER_VALUES"

    def _is_sparse_sports_entity(self, venue: str) -> bool:
        """Check if this is a sparse sports entity with wrong fixture-calendar denominator."""
        return venue.upper() in self._SPARSE_SPORTS_ENTITIES

    # ── Reference-data-driven expected dates ──────────────────────────────

    # Cache for upstream reference data (keyed by upstream:category:date_range)
    _REF_DATA_CACHE: ClassVar[dict[str, tuple[float, dict[str, set[str]]]]] = {}
    _REF_DATA_CACHE_TTL = 300  # 5 minutes

    def _get_reference_expected_dates(
        self,
        category: str,
        start_date: str,
        end_date: str,
        service: str = "",
        cloud: str = "gcp",
    ) -> dict[str, set[str]]:
        """Read upstream service availability index to get per-venue expected dates.

        Uses the denominator chain: each service's expected dates come from its
        direct upstream (``_UPSTREAM_SERVICE_MAP``).  Falls back to
        instruments-service if no upstream is defined.

        Returns {venue: {date_str, ...}} — the set of dates where the upstream
        service has data for that venue.
        """
        upstream = self._UPSTREAM_SERVICE_MAP.get(service, "instruments-service")
        cache_key = f"{upstream}:{category}:{start_date}:{end_date}"
        now = time.monotonic()
        cached = self._REF_DATA_CACHE.get(cache_key)
        if cached and (now - cached[0]) < self._REF_DATA_CACHE_TTL:
            return cached[1]

        if upstream == "features-commodity-service":
            bucket = COMMODITY_BUCKET_TEMPLATE.format(pid=self.project_id)
        else:
            upstream_kind = SERVICE_TO_KIND.get(upstream, "")
            if not upstream_kind:
                return {}
            ag = category.lower() or None
            if ag == "prediction":
                pred_kind = PREDICTION_KIND_MAP.get(upstream_kind)
                bucket = _dss.resolve_bucket_name(cloud=cloud, kind=pred_kind if pred_kind else upstream_kind)  # pyright: ignore[reportArgumentType]
            else:
                bucket = _dss.resolve_bucket_name(cloud=cloud, kind=upstream_kind, asset_group=ag)  # pyright: ignore[reportArgumentType]

        result: dict[str, set[str]] = {}
        try:
            idx = _dss._read_index_cached(bucket)  # pyright: ignore[reportPrivateUsage]  # facade patch-point (late-bound)
            if idx.empty:
                return result
            mask = (idx["date"] >= start_date) & (idx["date"] <= end_date)
            # Filter by upstream service_name if shared bucket
            if "service_name" in idx.columns:
                mask = mask & (idx["service_name"] == upstream)
            filtered = idx.loc[mask]
            if "venue" in filtered.columns:
                for v in filtered["venue"].unique():  # pyright: ignore[reportAny]
                    v_str = str(v)  # pyright: ignore[reportAny]
                    v_dates = {str(d) for d in filtered.loc[filtered["venue"] == v, "date"].unique()}  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType,reportUnknownArgumentType,reportAny,reportAttributeAccessIssue]
                    result[v_str] = v_dates
        except Exception:
            logger.debug("No upstream index for %s in %s", upstream, bucket)

        self._REF_DATA_CACHE[cache_key] = (now, result)
        return result

    # Denominator chain: each downstream service uses its direct upstream's
    # ACTUAL availability as the expected-date set.  Missing key = use
    # instruments-service (the root reference layer).
    _UPSTREAM_SERVICE_MAP: ClassVar[dict[str, str]] = {
        "market-tick-data-service": "instruments-service",
        "market-data-processing-service": "market-tick-data-service",
        "features-delta-one-service": "market-data-processing-service",
        "features-volatility-service": "market-data-processing-service",
        "features-onchain-service": "market-tick-data-service",
        "features-multi-timeframe-service": "market-data-processing-service",
        "features-cross-instrument-service": "market-data-processing-service",
        "features-sports-service": "market-tick-data-service",
        "features-calendar-service": "instruments-service",
        "features-commodity-service": "market-data-processing-service",
        # ml-training-service + ml-inference-service consolidated into ml-service (2026-05-21)
        "ml-service": "features-delta-one-service",
        "strategy-service": "ml-service",
        "execution-service": "strategy-service",
    }

    # Services whose expected-date denominator comes from upstream availability
    # rather than calendar trading days.
    _REFERENCE_DRIVEN_SERVICES: ClassVar[frozenset[str]] = frozenset(
        {
            "market-tick-data-service",
            "market-data-processing-service",
            "features-delta-one-service",
            "features-volatility-service",
            "features-onchain-service",
            "features-sports-service",
            "features-calendar-service",
            "features-multi-timeframe-service",
            "features-cross-instrument-service",
            "features-commodity-service",
            # ml-training-service + ml-inference-service consolidated into ml-service (2026-05-21)
            "ml-service",
            "strategy-service",
            "execution-service",
        }
    )
