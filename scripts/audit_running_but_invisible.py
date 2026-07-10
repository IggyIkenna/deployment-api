#!/usr/bin/env python3
# Epic: observability_master
# Lifecycle: one-shot audit
# Delete-when: the running-but-invisible report has been reviewed + any material gaps filed as todos.
"""One-shot audit — every billable-and-running resource per cloud vs. what the deployments tab shows.

WS-D #8. The deployments inventory censuses a fixed set of KINDS (VM / Cloud Run job+service /
Cloud Function / ECS / Lambda / DISK / STATIC_IP / SCHEDULER). This audit enumerates the WHOLE GCP
estate via **Cloud Asset Inventory** (the complete, credits-agnostic "what exists" source — REST +
ADC, no ``google-cloud-asset`` dep) and reports every asset TYPE the inventory does NOT cover, with
its live count, so a materially-costly running-but-invisible class (GKE cluster, Cloud SQL,
Dataflow/Composer, …) is caught rather than silently missing.

Materiality (operator-agreed 2026-07-10): a class worth ≈ ≥$5-10/mo per resource is added as a
census kind; the rest are filed as a follow-up. Asset Inventory gives TYPE + COUNT, not $/mo — the $
check is a manual pass against the billing export for each flagged class (this audit narrows WHICH
classes to check). Honest degradation: no creds / API disabled → a loud message, never a false "all
covered".

Run (live mode, ADC): ``.venv/bin/python scripts/audit_running_but_invisible.py --project <id>``.
"""

from __future__ import annotations

import argparse
import logging
import sys

logger = logging.getLogger("audit_running_but_invisible")

# GCP Cloud Asset Inventory asset types the deployments inventory ALREADY censuses → not "invisible".
_COVERED_GCP_ASSET_TYPES: frozenset[str] = frozenset(
    {
        "compute.googleapis.com/Instance",  # VM
        "run.googleapis.com/Job",  # CLOUD_RUN_JOB
        "run.googleapis.com/Service",  # CLOUD_RUN_SERVICE
        "cloudfunctions.googleapis.com/Function",  # CLOUD_FUNCTION (gen1)
        "cloudfunctions.googleapis.com/CloudFunction",  # CLOUD_FUNCTION (gen2)
        "compute.googleapis.com/Disk",  # DISK (orphaned)
        "compute.googleapis.com/RegionDisk",  # DISK (regional)
        "compute.googleapis.com/Address",  # STATIC_IP
        "compute.googleapis.com/GlobalAddress",  # STATIC_IP (global)
        "cloudscheduler.googleapis.com/Job",  # SCHEDULER
    }
)

_ASSET_API = "https://cloudasset.googleapis.com/v1/projects/{project}/assets"


def _search_all_asset_types(project: str) -> dict[str, int] | None:
    """Return ``{asset_type: count}`` for every live asset in ``project`` (or None on failure)."""
    try:
        import google.auth  # noqa: imports-inside-functions  # deferred: one-shot audit needs ADC only when run
        from google.auth.transport.requests import AuthorizedSession  # noqa: imports-inside-functions

        credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        session = AuthorizedSession(credentials)
    except Exception as exc:
        logger.error("ADC session unavailable — cannot audit (this is NOT 'all covered'): %s", exc)
        return None

    counts: dict[str, int] = {}
    url = _ASSET_API.format(project=project)
    page_token = ""
    try:
        while True:
            params = {"contentType": "RESOURCE", "pageSize": 500}
            if page_token:
                params["pageToken"] = page_token
            resp = session.get(url, params=params, timeout=60)
            if resp.status_code != 200:
                logger.error("Asset Inventory list -> HTTP %s: %s", resp.status_code, resp.text[:300])
                return None
            payload = resp.json()
            for asset in payload.get("assets") or []:
                asset_type = str(asset.get("assetType") or "")
                if asset_type:
                    counts[asset_type] = counts.get(asset_type, 0) + 1
            page_token = str(payload.get("nextPageToken") or "")
            if not page_token:
                break
    except Exception as exc:
        logger.error("Asset Inventory enumeration failed: %s", exc)
        return None
    return counts


def _report(counts: dict[str, int]) -> int:
    """Print the running-but-invisible report; return the number of uncovered classes found."""
    uncovered = {t: n for t, n in counts.items() if t not in _COVERED_GCP_ASSET_TYPES and n > 0}
    print(f"\n=== GCP estate audit — {len(counts)} asset types, {sum(counts.values())} resources ===")
    print(f"Covered by the deployments tab: {sorted(t for t in counts if t in _COVERED_GCP_ASSET_TYPES)}\n")
    if not uncovered:
        print("✅ No running-but-invisible resource classes — every live asset type maps to a censused kind.")
        return 0
    print("⚠️  RUNNING-BUT-INVISIBLE asset classes (verify $/mo materiality against the billing export):")
    for asset_type, count in sorted(uncovered.items(), key=lambda kv: (-kv[1], kv[0])):
        print(f"   {count:>5}  {asset_type}")
    print(
        "\nNext: for each class ≥ ~$5-10/mo per resource, add a census kind (or file a follow-up todo);"
        " the rest are acceptable to leave uncovered."
    )
    return len(uncovered)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True, help="GCP project id to audit")
    args = parser.parse_args()
    counts = _search_all_asset_types(args.project)
    if counts is None:
        print("❌ Audit could NOT run (no creds / API disabled) — this is not a clean result.", file=sys.stderr)
        return 2
    _report(counts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
