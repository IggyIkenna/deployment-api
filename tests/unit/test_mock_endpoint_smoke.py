# Epic: observability_master
"""Exhaustive mock-endpoint crash-smoke gate.

Auto-discovers every parameterless GET path from the app's own OpenAPI spec
(``deployment_api.main.app.openapi()``, same discovery rule as
``scripts/compare_live_mock_parity.py``'s Trap 4: paths with ``{param}`` need a
real id and are unmeasured by a parameterless sweep) and asserts none crash with
a 500 under ``CLOUD_MOCK_MODE=true``.

WHY THIS EXISTS
---------------
Prototyped 2026-08-03 (deployment_api_live_mock_parity_2026_07_17.md) as a
standalone script and shown to genuinely catch real bugs: it caught the
``/api/deployments/diff`` route-shadowing regression (see
``test_route_ordering_inventory.py``) and a permanent 500 on
``GET /api/user-management/users`` (``UnifiedCloudConfig().workspace_root`` —
an attribute that never existed) the same day. Wiring it into ``tests/unit`` (the
only tier ``quality-gates.sh`` runs) was shelved at the time because
``conftest.py``'s global service mocking produced one false-positive; that gap is
closed below rather than by upgrading the 5 globally-stubbed submodules to
AsyncMock-compatible doubles (a much larger, riskier change touching every other
test file that relies on the current bare-``MagicMock`` mocking).

WHY FULL LIFESPAN (``with TestClient(app) as client``), NOT THE FIXTURE-ONLY FORM
-----------------------------------------------------------------------------
Several routes read ``app.state.config_dir`` etc., which lifespan sets on
startup — a client built without entering the context manager (the pattern most
other route tests use) never runs lifespan and would under-cover the sweep. This
is the first ``tests/unit`` file to exercise full, un-mocked lifespan startup +
shutdown end-to-end. Verified safe under the actual CI/QG sandbox (no local
Redis): ``RedisCache.connect()`` (``deployment_api/utils/cache.py``) catches
``(OSError, ValueError, RuntimeError, RedisError)`` and logs a warning instead of
raising, so an unreachable Redis degrades to in-memory-only caching rather than
failing the lifespan — confirmed by running this exact sweep locally with no
Redis process running: only the one known artifact below crashed.

THE ONE KNOWN FALSE-POSITIVE (excluded, not fixed)
---------------------------------------------------
``GET /api/data-status/turbo/stats`` calls
``await data_status.data_analytics_service.get_cache_stats()``, where
``data_analytics_service`` is a package-level singleton
(``DataAnalyticsService()``) constructed at ROUTE-MODULE IMPORT TIME in
``deployment_api/routes/data_status/__init__.py``. Under this test tier's global
service mocking, ``DataAnalyticsService`` is a bare ``MagicMock`` class (not
``AsyncMock``), so the singleton's ``get_cache_stats()`` returns a plain
``MagicMock`` — ``await``-ing it raises ``TypeError``, which the route's
``except (OSError, ValueError, RuntimeError)`` clause does not catch (by design;
a real ``TypeError`` there would be a genuine bug, not something to swallow). This
is an artifact of the global test-mock, not evidence of a live/mock defect:
``test_route_data_status_live.py`` already exercises both the success and
failure paths of this exact endpoint by patching ``data_analytics_service``
per-test with a real ``AsyncMock``. Scoping the sweep to skip this one path (per
the todo's second option — "scope the sweep to skip routes that import from
those specific submodules") avoids a much larger AsyncMock-upgrade change to
``conftest.py``'s 5 globally-stubbed submodules
(``data_analytics_service``/``data_query_service``/``data_status_service``/
``deployment_manager``/``deployment_state``) for a single affected route.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

from deployment_api.main import app

# See "THE ONE KNOWN FALSE-POSITIVE" above — grow this set only with the same
# investigation depth (confirm the crash is a global-mock artifact, not a real
# route bug, and cite the existing dedicated test file that covers the endpoint
# for real). A path landing here silently is exactly the kind of drift this gate
# exists to prevent.
_KNOWN_MOCK_ARTIFACT_PATHS = frozenset(
    {
        "/api/data-status/turbo/stats",
    }
)


def _discover_parameterless_get_paths() -> list[str]:
    """Every GET path in the OpenAPI spec with no ``{param}`` segment."""
    spec = app.openapi()
    return sorted(path for path, methods in spec["paths"].items() if "get" in methods and "{" not in path)


_ALL_PATHS = _discover_parameterless_get_paths()


@pytest.fixture(scope="module")
def _client() -> Iterator[TestClient]:
    """Full-lifespan client — see module docstring for why this must enter the context manager."""
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


@pytest.mark.parametrize("path", _ALL_PATHS)
def test_mock_endpoint_does_not_crash(_client: TestClient, path: str) -> None:
    """Every parameterless GET must not 500 under CLOUD_MOCK_MODE=true.

    A 500 here means the mock path itself is broken — a UI/Playwright session or
    an offline dev developing against mock mode would hit this same crash. Any
    other status (200, 422 for missing query params, 404, 501 for a deliberately
    unimplemented mock branch, etc.) is fine; this gate only catches crashes, not
    contract shape (that is ``compare_live_mock_parity.py``'s job, and needs live
    infra this test tier doesn't have).
    """
    if path in _KNOWN_MOCK_ARTIFACT_PATHS:
        pytest.skip(f"{path}: known conftest-mock artifact — see module docstring")

    response = _client.get(path)
    assert response.status_code != 500, f"{path} returned 500 under CLOUD_MOCK_MODE=true: {response.text[:500]}"


def test_sweep_covers_a_meaningful_number_of_endpoints() -> None:
    """Guards against the discovery silently degrading to near-zero (e.g. a broken openapi() call)."""
    assert len(_ALL_PATHS) > 50, (
        f"Only {len(_ALL_PATHS)} parameterless GET paths discovered — expected 100+; "
        "the OpenAPI-based discovery may be broken."
    )
