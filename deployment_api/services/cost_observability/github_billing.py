"""Real GitHub billing provider — the Enhanced Billing usage report → normalized CostRecord.

GitHub's Enhanced Billing usage endpoint
(``GET /users/{user}/settings/billing/usage`` or ``/organizations/{org}/settings/billing/usage``)
returns per-day x product x repo x SKU line items, each carrying a net USD ``netAmount`` - the same
granularity the GCP/AWS providers give, so it maps straight onto ``CostRecord``. A window can span two
calendar months, so we query per (year, month) and keep items whose ``date`` falls in ``[start, end)``.

Requires a token with the **"Plan" account permission** (fine-grained PAT) or classic ``user`` scope;
without it every billing endpoint returns **403**, so :func:`fetch_github_billing` returns ``None`` and
the caller (``providers.github_facts``) falls back to the labelled dummy — the page never shows a bare
or fabricated GitHub figure. The token is read from Secret Manager via ``get_secret_client`` (a dedicated
billing secret first, then the shared ``GH_PAT``) — never from the raw process environment.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import cast

import httpx
from unified_trading_library import get_secret_client

from deployment_api.deployment_api_config import DeploymentApiConfig
from deployment_api.services.cost_observability.models import CLOUD_GITHUB, KIND_OTHER, CostRecord

logger = logging.getLogger(__name__)

_API_BASE = "https://api.github.com"
_API_VERSION = "2022-11-28"
_TIMEOUT = 20.0
# GitHub returns products lowercase ("actions", "copilot", …); present them like the proper-cased
# GCP/AWS service names so the cross-cloud breakdown reads consistently.
_PRODUCT_LABELS = {
    "actions": "Actions",
    "copilot": "Copilot",
    "packages": "Packages",
    "codespaces": "Codespaces",
    "storage": "Storage",
    "git_lfs": "Git LFS",
}
# The shared CI/repo token — tried only after the dedicated billing secret. It backs repo-ci and
# usually LACKS the Plan scope (billing 403s), but is a harmless last resort if it ever gains it.
_FALLBACK_SECRET = "GH_PAT"


def _as_float(v: object) -> float:
    if isinstance(v, bool):
        return float(v)
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str) and v.strip():
        try:
            return float(v)
        except ValueError:
            return 0.0
    return 0.0


def _billing_token(config: DeploymentApiConfig) -> str | None:
    """Plan-scoped billing token from Secret Manager (dedicated billing secret, then shared GH_PAT)."""
    project_id = config.gcp_project_id
    if not project_id:
        return None
    client = get_secret_client(project_id=project_id)
    for name in (config.github_billing_secret, _FALLBACK_SECRET):
        try:
            token = client.get_secret(name)
        except Exception as exc:  # secret absent / no access — try the next candidate
            logger.debug("GitHub billing secret %s unavailable: %s", name, exc)
            continue
        if token:
            return token
    return None


def _next_month(y: int, m: int) -> tuple[int, int]:
    return (y + 1, 1) if m == 12 else (y, m + 1)


def _prev_month(y: int, m: int) -> tuple[int, int]:
    return (y - 1, 12) if m == 1 else (y, m - 1)


def _months(start: date, end: date) -> list[tuple[int, int]]:
    """(year, month) pairs the ``[start, end)`` window touches (``end`` exclusive)."""
    # end is exclusive; a window ending on the 1st does not touch that month.
    last = (end.year, end.month) if end.day > 1 else _prev_month(end.year, end.month)
    out: list[tuple[int, int]] = []
    cur = (start.year, start.month)
    while cur <= last:
        out.append(cur)
        cur = _next_month(*cur)
    return out


def _billing_url(config: DeploymentApiConfig) -> str:
    owner = "organizations" if config.github_billing_account_type == "org" else "users"
    return f"{_API_BASE}/{owner}/{config.github_billing_account}/settings/billing/usage"


def _map_usage_items(payload: object, start: date, end: date) -> list[CostRecord]:
    """Enhanced-billing ``usageItems`` → CostRecord, keeping only items inside ``[start, end)``.

    ``netAmount`` is the invoiced USD after discounts; ``grossAmount`` the pre-discount list cost.
    Mirroring GCP/AWS: ``cost`` = gross, ``credit`` = net - gross (<= 0), so net = cost + credit.
    """
    if not isinstance(payload, dict):
        return []
    data = cast("dict[str, object]", payload)
    raw_items = data.get("usageItems")
    if not isinstance(raw_items, list):
        return []
    out: list[CostRecord] = []
    for raw in cast("list[object]", raw_items):
        if not isinstance(raw, dict):
            continue
        item = cast("dict[str, object]", raw)
        day = str(item.get("date") or "")
        try:
            d = date.fromisoformat(day[:10])
        except ValueError:
            continue
        if not (start <= d < end):
            continue
        net = _as_float(item.get("netAmount"))
        gross = _as_float(item.get("grossAmount"))
        if gross == 0.0 and net != 0.0:  # some items report net only
            gross = net
        credit = round(net - gross, 6)  # ≤ 0 discount
        raw_product = str(item.get("product") or "GitHub")
        product = _PRODUCT_LABELS.get(raw_product.lower(), raw_product.replace("_", " ").title())
        repo = str(item.get("repositoryName") or "")
        resource_id = repo or product
        out.append(
            CostRecord(
                cloud=CLOUD_GITHUB,
                day=d.isoformat(),
                service=product,
                resource_id=resource_id,
                resource_kind=KIND_OTHER,
                region="global",
                cost=round(gross, 6),
                credit=credit,
                currency="USD",  # GitHub billing is USD-native → native == USD
                cost_native=round(gross, 6),
                credit_native=credit,
                sku=str(item.get("sku") or ""),
            )
        )
    return out


def fetch_github_billing(config: DeploymentApiConfig, start: date, end: date) -> list[CostRecord] | None:
    """Real per-day GitHub cost from the Enhanced Billing usage report, or ``None`` if unavailable.

    ``None`` (→ caller falls back to the dummy) when no token is configured or **every** billing call
    fails/403s; ``[]`` only when the account genuinely has no billed usage in the window.
    """
    token = _billing_token(config)
    if not token:
        logger.info("GitHub billing: no Plan-scoped token reachable — using the labelled dummy")
        return None
    url = _billing_url(config)
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": _API_VERSION,
    }
    records: list[CostRecord] = []
    any_ok = False
    with httpx.Client(timeout=_TIMEOUT) as client:
        for year, month in _months(start, end):
            try:
                resp = client.get(url, headers=headers, params={"year": year, "month": month})
            except httpx.HTTPError as exc:
                logger.warning("GitHub billing request failed (%d-%02d): %s", year, month, exc)
                continue
            if resp.status_code == 403:
                logger.warning("GitHub billing 403 — token lacks the 'Plan' permission; using the dummy")
                return None
            if resp.status_code != 200:
                logger.warning("GitHub billing %d-%02d → HTTP %d", year, month, resp.status_code)
                continue
            any_ok = True
            records.extend(_map_usage_items(cast(object, resp.json()), start, end))  # noqa: qg-raw-json
    return records if any_ok else None
