"""CSV / fixture / shard download data-status routes + capture-status response builders.

Split from ``routes/data_status.py`` (pure code motion; plan:
``codex_violations_ratchet_to_five_2026_06_10.md`` Phase-1 P2). Routes
register on the package facade's shared ``router``; patched module-level
collaborators are resolved through the facade module (``_ds``) at call
time so the existing test patch surface keeps intercepting.
"""

import logging

from fastapi import HTTPException, Query
from fastapi.responses import Response

import deployment_api.routes.data_status as _ds
from deployment_api.routes.data_status import router
from deployment_api.services.data_status_drilldown import (
    MTDS_SHARD_SERVICES,
    build_csv_export,
    build_fixture_breakdown,
    build_fixture_download,
    build_fixtures_csv_export,
    build_instruments_shard_csv_export,
    build_mtds_shard_csv_export,
    build_pool_breakdown,
    get_shard_info,
    lookup_capture_status_for_shard,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Capture-status download response branching (multi-axis drilldown).
#
# Plan: data_status_multi_axis_shard_propagation_2026_05_06.md Phase 3.
# The download endpoints below check the manifest's capture_status BEFORE
# attempting to read the parquet. The branch dictates HTTP status + body
# shape so the operator can tell:
#
#   captured + parquet OK   → 200 + CSV body                    (today)
#   empty_confirmed         → 200 + header-only CSV explaining the
#                             honest empty (source returned 0 rows).
#                             X-Capture-Status: empty_confirmed.
#   attempted_failed        → 200 + header-only CSV with the error
#                             reason. X-Capture-Status: attempted_failed.
#   captured + 0 byte read  → 502 + body explaining the path drift.
#                             X-Capture-Status: path_drift.
#                             Real bug class — manifest claims captured
#                             but the downloader can't find the parquet.
#   never_attempted         → 404 + body. X-Capture-Status: never_attempted.
# ---------------------------------------------------------------------------


def _empty_confirmed_csv_body(
    *,
    service: str,
    asset_group: str,
    day: str,
    error_reason: str,
    attempted_at: str,
) -> str:
    """Header-only CSV for empty_confirmed shards.

    Operators landing on this from the download button need to know
    immediately that the empty body is HONEST (source returned no
    rows), not a bug. We emit a CSV with one informative comment row
    so opening the file in any tool surfaces the message. The
    X-Capture-Status header carries the machine-readable signal.
    """
    return (
        "# CAPTURE_STATUS: empty_confirmed\n"
        f"# service: {service}\n"
        f"# asset_group: {asset_group}\n"
        f"# day: {day}\n"
        f"# attempted_at: {attempted_at}\n"
        f"# error_reason: {error_reason or '(none — source returned 0 rows)'}\n"
        "# This is an HONEST empty: the adapter ran successfully and the\n"
        "# upstream source returned zero rows for this shard. NaN downstream\n"
        "# is fine — tree-based ML and rank-based allocators handle missing\n"
        "# data natively. The denominator-clip rules in CLAUDE.md (sports\n"
        "# SOURCE_COVERAGE_START / VIX 15m layering / etc.) live here.\n"
    )


def _attempted_failed_csv_body(
    *,
    service: str,
    asset_group: str,
    day: str,
    error_reason: str,
    attempted_at: str,
) -> str:
    """Header-only CSV for attempted_failed shards."""
    return (
        "# CAPTURE_STATUS: attempted_failed\n"
        f"# service: {service}\n"
        f"# asset_group: {asset_group}\n"
        f"# day: {day}\n"
        f"# attempted_at: {attempted_at}\n"
        f"# error_reason: {error_reason or '(unclassified)'}\n"
        "# The adapter ran and raised. error_reason carries the classified\n"
        "# failure category. Re-run the orchestrator for this shard from the\n"
        "# Data Status tab; the manifest will flip back to captured on retry\n"
        "# success.\n"
    )


def _capture_status_response(
    *,
    service: str,
    asset_group: str,
    day: str,
    capture_meta: dict[str, str],
    filename_stem: str,
) -> Response | None:
    """Return a Response when the capture_status branches off the happy path.

    Returns None when status == ``captured`` so the caller falls through
    to its normal parquet-read code path. For every other status,
    returns the appropriate Response with X-Capture-Status header set.

    This is a pure response builder — does NOT touch the parquet.
    """
    status = capture_meta.get("status", "captured")
    error_reason = capture_meta.get("error_reason", "")  # noqa: qg-empty-fallback — optional manifest meta
    attempted_at = capture_meta.get("attempted_at", "")  # noqa: qg-empty-fallback — optional manifest meta

    if status == "captured":
        return None

    headers = {"X-Capture-Status": status}

    if status == "never_attempted":
        raise HTTPException(
            status_code=404,
            detail=(
                f"No manifest row for service={service}, asset_group={asset_group}, "
                f"day={day} — the adapter never ran for this shard. Trigger a "
                f"backfill from the Data Status tab."
            ),
            headers=headers,
        )

    if status == "empty_confirmed":
        body = _empty_confirmed_csv_body(
            service=service,
            asset_group=asset_group,
            day=day,
            error_reason=error_reason,
            attempted_at=attempted_at,
        )
        headers["Content-Disposition"] = f'attachment; filename="{filename_stem}.empty_confirmed.csv"'
        headers["X-Row-Count"] = "0"
        return Response(content=body, media_type="text/csv; charset=utf-8", headers=headers)

    if status == "attempted_failed":
        body = _attempted_failed_csv_body(
            service=service,
            asset_group=asset_group,
            day=day,
            error_reason=error_reason,
            attempted_at=attempted_at,
        )
        headers["Content-Disposition"] = f'attachment; filename="{filename_stem}.attempted_failed.csv"'
        headers["X-Row-Count"] = "0"
        return Response(content=body, media_type="text/csv; charset=utf-8", headers=headers)

    # Unknown status — treat as never_attempted but signal the surprise.
    raise HTTPException(
        status_code=500,
        detail=f"Unknown capture_status {status!r} from manifest",
        headers=headers,
    )


def _path_drift_response(
    *,
    service: str,
    asset_group: str,
    day: str,
    expected_path_hint: str,
) -> HTTPException:
    """Build the 502 response for the captured-but-0-rows path-drift case.

    Caller raises this when ``capture_status=captured`` per the manifest
    but the downloader's parquet probe returned 0 rows. Distinct from
    a 404 (manifest had nothing) and from a 200-empty (honest empty);
    the 502 deliberately signals "we believed we had data but the
    downloader can't deliver it" — operator-actionable bug.
    """
    return HTTPException(
        status_code=502,
        detail=(
            f"Manifest says captured for service={service}, "
            f"asset_group={asset_group}, day={day}, but the downloader read 0 "
            f"rows from {expected_path_hint or 'the expected path'}. Likely "
            f"writer/downloader path-template drift, chain-bundle / "
            f"instrument_type / hive-vocab axis drift, or a per-league / "
            f"per-job_id sub-partition the downloader doesn't walk into. Run "
            f"the phantom audit (see codex/02-data/availability-manifest-and-"
            f"data-status.md § Phantom audit — re-runnable recipe)."
        ),
        headers={"X-Capture-Status": "path_drift"},
    )


@router.get("/download-csv")
async def download_csv(
    service: str = Query(..., description="Service name"),
    asset_group: str = Query(..., description="Asset group"),
    venue: str = Query(..., description="Venue"),
    day: str = Query(..., description="Day (YYYY-MM-DD)"),
    instrument_type: str = Query(..., description="Instrument type"),
    data_type: str = Query(..., description="Data type"),
    instrument_ids: str = Query("", description="Comma-separated instrument IDs (empty = all)"),
    chain: str | None = Query(None, description="DeFi chain leaf-axis filter"),
    league_id: str | None = Query(None, description="Sports league_id leaf-axis filter"),
    job_id: str | None = Query(None, description="ML / strategy / execution job_id leaf-axis filter"),
    search: str | None = Query(
        None,
        description=(
            "Case-insensitive substring match on instrument_id, applied ONLY when "
            "instrument_ids is empty (matches the on-screen filtered view)."
        ),
    ),
    mvp_only: bool = Query(
        False,
        description="Restrict the export to is_mvp-true instruments (same rule as instrument_ids).",
    ),
):
    """Stream a CSV of the selected instruments for one shard.

    Reads the manifest's capture_status for the requested axis tuple
    FIRST and branches before touching the parquet — see
    ``_capture_status_response`` for the full empty_confirmed /
    attempted_failed / never_attempted / path_drift behaviour.

    Empty ``instrument_ids`` means "download the full shard" — ``search`` /
    ``mvp_only`` then narrow that to match the on-screen filtered instrument
    list exactly (P6 phase-1). The server caps output at 500k rows — larger
    requests get a 413 advising BigQuery external tables.
    """
    capture_meta = lookup_capture_status_for_shard(
        service=service,
        asset_group=asset_group,
        day=day,
        venue=venue,
        instrument_type=instrument_type,
        data_type=data_type,
        chain=chain,
        league_id=league_id,
        job_id=job_id,
    )
    branched = _capture_status_response(
        service=service,
        asset_group=asset_group,
        day=day,
        capture_meta=capture_meta,
        filename_stem=f"{service}_{asset_group}_{venue}_{day}",
    )
    if branched is not None:
        return branched

    ids = [s.strip() for s in instrument_ids.split(",") if s.strip()] if instrument_ids else []
    try:
        csv_text, row_count, filename = build_csv_export(
            service=service,
            asset_group=asset_group,
            venue=venue,
            day=day,
            instrument_type=instrument_type,
            data_type=data_type,
            instrument_ids=ids,
            search=search,
            mvp_only=mvp_only,
        )
    except ValueError as e:
        # Row-cap exceeded.
        raise HTTPException(status_code=413, detail=str(e)) from e
    except (OSError, RuntimeError) as e:
        logger.exception("Error in download_csv")
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from e

    if row_count == 0:
        # Manifest said captured, parquet read returned 0 rows → path drift.
        raise _path_drift_response(
            service=service,
            asset_group=asset_group,
            day=day,
            expected_path_hint=filename,
        )

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "X-Row-Count": str(row_count),
        "X-Capture-Status": "captured",
    }
    return Response(content=csv_text, media_type="text/csv; charset=utf-8", headers=headers)


@router.get("/download-fixtures-csv")
async def download_fixtures_csv(
    day: str = Query(..., description="Day (YYYY-MM-DD)"),
    league_id: str = Query(..., description="Canonical league_id (e.g. EPL)"),
):
    """Stream a CSV of all fixtures for one (day, league) slice.

    Sports FIXTURES are stored as a single per-day parquet at
    ``sports_reference/by_date/day={day}/entity=fixtures/fixtures.parquet``
    keyed by API-Football numeric league id. This endpoint filters by
    canonical ``league_id`` via UAC and streams the matching rows as CSV.

    Reads the manifest capture_status for this (instruments-service,
    sports, day, league_id) FIRST and branches — empty_confirmed leagues
    (paused window per UAC ``KNOWN_COVERAGE_GAPS`` or pre-source-launch
    per ``SOURCE_COVERAGE_START``) return a header-only CSV explaining
    the honest empty rather than a confusing zero-row download.
    """
    capture_meta = lookup_capture_status_for_shard(
        service="instruments-service",
        asset_group="sports",
        day=day,
        data_type="fixtures",
        league_id=league_id,
    )
    branched = _capture_status_response(
        service="instruments-service",
        asset_group="sports",
        day=day,
        capture_meta=capture_meta,
        filename_stem=f"fixtures_{league_id}_{day}",
    )
    if branched is not None:
        return branched

    try:
        csv_text, row_count, filename = build_fixtures_csv_export(
            day=day,
            league_id=league_id,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except (OSError, RuntimeError) as e:
        logger.exception("Error in download_fixtures_csv")
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from e

    if row_count == 0:
        # Manifest claimed captured for this (day, league_id) but the
        # parquet probe returned 0 rows. Distinct from the honest-empty
        # branch above — file as path drift so the operator notices.
        raise _path_drift_response(
            service="instruments-service",
            asset_group="sports",
            day=day,
            expected_path_hint=filename,
        )

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "X-Row-Count": str(row_count),
        "X-Capture-Status": "captured",
    }
    return Response(content=csv_text, media_type="text/csv; charset=utf-8", headers=headers)


@router.get("/download-shard-csv")
async def download_shard_csv(
    service: str = Query(..., description="Service name (e.g. instruments-service)"),
    asset_group: str = Query(..., description="Asset group (CEFI, TRADFI, DEFI)"),
    venue: str = Query(..., description="Venue name (e.g. BINANCE-SPOT)"),
    date: str = Query(..., description="Day (YYYY-MM-DD)"),
    chain: str | None = Query(None, description="DeFi chain leaf-axis filter"),
    league_id: str | None = Query(None, description="Sports league_id leaf-axis filter"),
    job_id: str | None = Query(None, description="ML / strategy / execution job_id leaf-axis filter"),
):
    """Stream a CSV for one (service, asset group, venue, date) shard or catalog.

    Reads the manifest's capture_status FIRST and branches before
    touching the parquet — empty_confirmed and attempted_failed return
    200 with header-only bodies explaining the status; never_attempted
    returns 404; captured-but-zero-rows returns 502 (path drift) so the
    operator distinguishes "honest empty" from "downloader broken".
    See ``_capture_status_response`` for the full contract.

    Routing for the captured happy path:
    * instruments-service / corporate-actions — reads the per-(venue, day)
      ``instruments.parquet`` bundle and returns its rows as CSV.
    * market-tick-data-service / market-data-processing-service — reads the
      availability manifest catalog filtered to ``(venue, date)`` and returns
      instrument capture metadata as CSV (tick data itself is too large).
    """
    capture_meta = lookup_capture_status_for_shard(
        service=service,
        asset_group=asset_group,
        day=date,
        venue=venue,
        chain=chain,
        league_id=league_id,
        job_id=job_id,
    )
    branched = _capture_status_response(
        service=service,
        asset_group=asset_group,
        day=date,
        capture_meta=capture_meta,
        filename_stem=f"{service}_{asset_group}_{venue}_{date}",
    )
    if branched is not None:
        return branched

    try:
        svc = service.lower()
        if svc in MTDS_SHARD_SERVICES:
            csv_text, row_count, filename = build_mtds_shard_csv_export(
                service=service,
                category=asset_group,
                venue=venue,
                date=date,
            )
        else:
            csv_text, row_count, filename = build_instruments_shard_csv_export(
                service=service,
                asset_group=asset_group,
                venue=venue,
                date=date,
                chain=chain,
            )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except (OSError, RuntimeError) as e:
        logger.exception("Error in download_shard_csv")
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from e

    # Manifest said captured but parquet read returned 0 rows → path drift.
    # Distinct from the honest empty / never_attempted branches handled
    # above — the operator needs to know we believed we had data and
    # couldn't deliver it.
    if row_count == 0 and not csv_text:
        raise _path_drift_response(
            service=service,
            asset_group=asset_group,
            day=date,
            expected_path_hint=filename,
        )

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "X-Row-Count": str(row_count),
        "X-Capture-Status": "captured",
    }
    return Response(content=csv_text, media_type="text/csv; charset=utf-8", headers=headers)


@router.get("/fixtures/breakdown")
async def get_fixture_breakdown(
    day: str = Query(..., description="Day (YYYY-MM-DD)"),
    league_id: str = Query(..., description="Canonical league_id (e.g. EPL)"),
):
    """Per-fixture coverage breakdown for one (day, league_id).

    Sports is fixture-anchored — this endpoint exposes the fixture leaf one
    level below the existing per-(league, day) drilldown. For every fixture
    on the day, returns per-entity capture status so the UI can render
    green/amber/red badges per fixture and wire per-fixture download links.

    Response shape documented on ``build_fixture_breakdown``.
    """
    try:
        return build_fixture_breakdown(day=day, league_id=league_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except (OSError, RuntimeError) as e:
        logger.exception("Error in get_fixture_breakdown")
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from e


@router.get("/pools/breakdown")
async def get_pool_breakdown(
    day: str = Query(..., description="Day (YYYY-MM-DD)"),
    venue: str = Query(..., description="DeFi venue (e.g. UNISWAP_V3, AAVE_V3, LIDO)"),
    chain: str = Query(..., description="Chain (e.g. ETHEREUM, ARBITRUM, POLYGON, BASE)"),
):
    """Per-pool coverage breakdown for one (day, venue, chain) DeFi shard.

    Institutional equivalent of ``/fixtures/breakdown`` for chain-native data.
    Walks every ``(instrument_type, data_type)`` partition under the
    ``venue=<VENUE>-<CHAIN>/`` GCS prefix, extracts the canonical pool / asset
    identifier from each parquet, and returns a per-pool coverage map showing
    which data_types captured each pool.

    Response shape documented on ``build_pool_breakdown``.
    """
    try:
        return build_pool_breakdown(day=day, venue=venue, chain=chain)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except (OSError, RuntimeError) as e:
        logger.exception("Error in get_pool_breakdown")
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from e


@router.get("/fixtures/download")
async def download_fixture_payload(
    fixture_id: str = Query(..., description="Canonical fixture_id"),
    day: str = Query(..., description="Kickoff day (YYYY-MM-DD) — used to locate parquets"),
    format: str = Query("csv", description="'csv' or 'json'"),
):
    """Stream a per-fixture union download across every fixture-scoped entity.

    Reads the 8 per-day fixture-scoped entity parquets, filters each to
    ``fixture_id``, and returns either:

    - **CSV**: denormalised — one leading ``entity`` column + each entity's
      columns concatenated (union-of-columns, missing values blank).
    - **JSON**: ``{fixture_id, day, coverage: {...}, entities: {...}}`` —
      each entity value is either a ``{capture_status, rows}`` dict (when
      captured) or a ``{capture_status}`` sentinel.
    """
    try:
        body_text, row_count, filename, media_type = build_fixture_download(
            fixture_id=fixture_id,
            day=day,
            fmt=format,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except (OSError, RuntimeError) as e:
        logger.exception("Error in download_fixture_payload")
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from e

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "X-Row-Count": str(row_count),
    }
    return Response(content=body_text, media_type=media_type, headers=headers)


@router.get("/shard-info")
async def get_shard_info_endpoint(
    service: str = Query(..., description="Service name"),
    asset_group: str = Query(..., description="Asset group"),
    venue: str = Query(..., description="Venue"),
    day: str = Query(..., description="Day (YYYY-MM-DD)"),
    data_type: str = Query(..., description="Data type"),
):
    """Return the instrument_types present on a venue+day+data_type shard.

    The UI uses this to resolve the ``instrument_type`` axis before opening
    the Instruments modal — avoids guessing ``data_type`` as the
    instrument_type for venues that actually shard by it (e.g. DERIBIT's
    ``options_chain`` vs ``perpetual``).

    Response:
    ``{instrument_types: [{name, bundling}, ...], recommended_instrument_type}``.
    """
    try:
        return get_shard_info(
            service=service,
            asset_group=asset_group,
            venue=venue,
            day=day,
            data_type=data_type,
        )
    except (OSError, ValueError, RuntimeError) as e:
        logger.exception("Error in get_shard_info_endpoint")
        raise HTTPException(status_code=500, detail="Internal server error. Check server logs.") from e


@router.post("/drilldown/clear-cache")
async def clear_drilldown_cache_endpoint():
    """Reset the drill-down TTL cache (schema / instruments / bucket counts)."""
    _ds.clear_drilldown_cache()
    return {"status": "ok"}
