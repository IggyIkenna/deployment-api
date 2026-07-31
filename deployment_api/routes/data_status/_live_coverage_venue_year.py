"""§4 venue x year coverage breakdown data-status route (deployment_ui plan §4 P1).

Split 2026-07-31 (``deployment_api_qg_size_gate_debt_2026_07_30.md``) out of
``_live_coverage.py`` (920L, over the 900L file-size cap) — pure code motion,
no logic changes. Patched module-level collaborators are resolved through the
facade module (``_ds``) at call time so the existing test patch surface keeps
intercepting.
"""

import logging

import pandas as pd
from fastapi import Query
from pydantic import BaseModel, Field

import deployment_api.routes.data_status as _ds
from deployment_api.routes.data_status import router
from deployment_api.routes.data_status._coverage_scope import (
    ConfigVersionTriple,
    CoverageScope,
    config_versions,
    filter_to_mvp,
)
from deployment_api.services.data_status_union import (
    has_provenance_columns,
    provenance_breakdown,
    union_reduce_to_cells,
)

logger = logging.getLogger(__name__)

_VENUE_YEAR_COVERAGE_ASSET_GROUPS = ("cefi", "tradfi", "defi", "sports", "prediction")
_BLOCKED_CREDENTIALS_REASON = "blocked_credentials"


class VenueYearRow(BaseModel):  # CORRECT-LOCAL: deployment-ui venue-year drilldown row; TS consumer only
    venue: str
    asset_group: str
    year: int
    captured: int = 0
    empty_confirmed: int = 0
    expected_unattempted: int = 0
    pending_paid_key: int = 0
    attempted_failed: int = 0
    total: int = 0

    @property
    def remaining(self) -> int:
        return self.total - self.captured - self.empty_confirmed - self.expected_unattempted


class VenueYearCoverageResponse(
    BaseModel
):  # CORRECT-LOCAL: deployment-ui venue-year response envelope; TS consumer only
    rows: list[VenueYearRow]
    asset_groups_loaded: list[str]
    asset_groups_failed: list[str]
    scope: CoverageScope = "could_exist"
    config_versions: dict[str, ConfigVersionTriple] = Field(default_factory=config_versions)
    source_breakdown: list[dict[str, object]] = Field(
        default_factory=list,
        description=(
            "FLAG-1 per-(pipeline_mode, source) capture-status CELL breakdown across the "
            "loaded asset_groups (CeFi multi-source UNION provenance). Empty on a v8 "
            "manifest carrying no source/pipeline_mode columns."
        ),
    )


@router.get("/venue-year-coverage", response_model=VenueYearCoverageResponse)
async def get_venue_year_coverage(
    asset_groups: str = Query(
        "cefi,tradfi,defi",
        description="Comma-separated asset groups (cefi/tradfi/defi/sports/prediction)",
    ),
    scope: CoverageScope = Query(
        "could_exist",
        description=(
            "Coverage scope (denominator filter). 'could_exist' (DEFAULT — current "
            "behaviour) = the full 4-state could-exist denominator. 'all' = the full "
            "universe (== could_exist at this endpoint). 'mvp' = restrict to cells where "
            "UAC is_mvp(...) is True (MVP-readiness board)."
        ),
    ),
) -> VenueYearCoverageResponse:
    """Per-venue x year capture-status breakdown from the MTDS availability manifest.

    Reads ``_index/availability_index.parquet`` for each requested asset group
    and aggregates by (venue, year, capture_status).  The UI uses this to show:

    * **captured** — shards successfully downloaded.
    * **empty_confirmed** — genuinely absent (pre-listing, out-of-window, venue gap).
    * **expected_unattempted** — known-missing, not yet attempted.
    * **pending_paid_key** — ``attempted_failed`` + ``error_reason=blocked_credentials``
      (401 / expired Tardis key).  A cell with this reason must read
      "downloadable once key active", NOT "complete/empty".
    * **attempted_failed** — other failures (network, parse error, etc.).
    * **remaining** (derived) = total - captured - empty_confirmed - expected_unattempted.

    Source: ``_index/availability_index.parquet`` per MTDS bucket.

    Stale-tolerant read (item 5a): the manifest is read via the stale-tolerant
    monitoring reader — on an EMPTY live result it falls back to the consolidated
    ``_index`` blob DIRECTLY (no live-freshness gate), so a paused/stale
    consolidator does not blank the board (migration-safe, read-only).

    FLAG-1 multi-source UNION (item 5b): when the manifest carries provenance
    columns (``source`` / ``pipeline_mode`` — CeFi multi-source), the rows are
    UNION-reduced to ONE honest cell first (≥1 source ``captured`` ⇒ the cell is
    ``captured``), and a per-(pipeline_mode, source) ``source_breakdown`` is
    surfaced alongside the counts.

    Scope toggle (Item 4): ``scope=mvp`` restricts the denominator + numerator to
    UAC ``is_mvp(...)`` cells; ``could_exist`` (default) / ``all`` keep the full
    universe. The per-config ``config_versions`` triples are returned so a
    coverage delta attributes to a scope-change vs a data-change.
    """
    requested_ags = [ag.strip().lower() for ag in asset_groups.split(",") if ag.strip()]
    requested_ags = [ag for ag in requested_ags if ag in _VENUE_YEAR_COVERAGE_ASSET_GROUPS]

    rows: list[VenueYearRow] = []
    loaded: list[str] = []
    failed: list[str] = []
    source_breakdown: list[dict[str, object]] = []

    for ag in requested_ags:
        try:
            bucket = _ds.build_bucket_name("market-tick-data-service", ag)
            # Stale-tolerant monitoring read (item 5a): empty live → consolidated
            # ``_index`` blob directly. Resolved through the facade so the test
            # patch surface intercepts.
            df: pd.DataFrame = _ds._read_manifest_index(bucket)  # pyright: ignore[reportPrivateUsage]
        except Exception as exc:
            logger.warning("venue-year-coverage: failed to read manifest for %s: %s", ag, exc)
            failed.append(ag)
            continue

        if df.empty or "date" not in df.columns:
            loaded.append(ag)
            continue

        # FLAG-1 (item 5b): per-(pipeline_mode, source) breakdown BEFORE the union
        # collapse, so a multi-source CeFi cell surfaces each source's answer.
        if has_provenance_columns(df):
            for entry in provenance_breakdown(df):
                entry["asset_group"] = ag.upper()
                source_breakdown.append(entry)
            # UNION-reduce multi-(source, pipeline_mode) rows to ONE honest cell so
            # the venue x year counts are cell-grain (≥1 source captured ⇒ captured),
            # never row-grain double-counted across provenance rows.
            df = union_reduce_to_cells(df)
            if df.empty or "date" not in df.columns:
                loaded.append(ag)
                continue

        # Scope filter (Item 4): mvp → keep only UAC is_mvp cells. could_exist / all
        # keep the full enumerated universe (the manifest IS that universe).
        if scope == "mvp":
            df = filter_to_mvp(df, ag)
            if df.empty:
                loaded.append(ag)
                continue

        # Derive year from date column (YYYY-MM-DD or datetime).
        df = df.copy()
        df["_year"] = pd.to_datetime(df["date"], errors="coerce").dt.year
        df = df.dropna(subset=["_year", "venue"])
        if df.empty:
            loaded.append(ag)
            continue

        df["_year"] = df["_year"].astype(int)
        df["venue"] = df["venue"].astype(str).str.upper()

        # Normalise capture_status: default missing to "captured" (legacy rows).
        if "capture_status" not in df.columns:
            df["capture_status"] = "captured"
        else:
            df["capture_status"] = df["capture_status"].fillna("captured").astype(str)

        error_col = df["error_reason"].astype(str) if "error_reason" in df.columns else pd.Series("", index=df.index)

        # Classify: attempted_failed + blocked_credentials → pending_paid_key.
        def _classify(row: "pd.Series[object]") -> str:  # type: ignore[type-arg]
            status = str(row["capture_status"]).lower()
            if status == "attempted_failed":
                reason = str(row.get("_error_reason", "") or "").lower()  # noqa: qg-empty-fallback — optional manifest column
                if _BLOCKED_CREDENTIALS_REASON in reason:
                    return "pending_paid_key"
            return status

        df["_error_reason"] = error_col
        df["_status_key"] = df.apply(_classify, axis=1)

        # Aggregate per (venue, year, status_key).
        agg = df.groupby(["venue", "_year", "_status_key"]).size().reset_index(name="count")

        # Also compute total per (venue, year).
        totals = df.groupby(["venue", "_year"]).size().reset_index(name="total")
        totals_map: dict[tuple[str, int], int] = {
            (str(r["venue"]), int(r["_year"])): int(r["total"]) for _, r in totals.iterrows()
        }

        # Build result rows — one VenueYearRow per (venue, year).
        bucket_map: dict[tuple[str, int], dict[str, int]] = {}
        for _, agg_row in agg.iterrows():
            key = (str(agg_row["venue"]), int(agg_row["_year"]))
            if key not in bucket_map:
                bucket_map[key] = {}
            bucket_map[key][str(agg_row["_status_key"])] = int(agg_row["count"])

        for (venue, year), counts in sorted(bucket_map.items()):
            rows.append(
                VenueYearRow(
                    venue=venue,
                    asset_group=ag.upper(),
                    year=year,
                    captured=counts.get("captured", 0),
                    empty_confirmed=counts.get("empty_confirmed", 0),
                    expected_unattempted=counts.get("expected_unattempted", 0),
                    pending_paid_key=counts.get("pending_paid_key", 0),
                    attempted_failed=counts.get("attempted_failed", 0),
                    total=totals_map.get((venue, year), 0),
                )
            )

        loaded.append(ag)

    return VenueYearCoverageResponse(
        rows=rows,
        asset_groups_loaded=loaded,
        asset_groups_failed=failed,
        scope=scope,
        config_versions=config_versions(),
        source_breakdown=source_breakdown,
    )
