# Epic: observability_master
"""Regression: literal /api/deployments/* routes must not be shadowed.

Caught on real cloud 2026-06-24: ``GET /api/deployments/inventory`` was routed to
the parametric ``GET /api/deployments/{deployment_id}`` (get_deployment_status)
because the parametric router was registered FIRST — so it loaded a deployment
literally named ``inventory`` → 404 ``state.json`` → 500, which broke the cockpit
Live/Batch/Paper tabs. FastAPI matches routes in registration order, so the
inventory router (literal routes) MUST register before deployments.router (the
parametric ``/{deployment_id}``). This guards the ordering so the regression
can't silently come back.

Same class caught again 2026-08-03 via the mock-endpoint smoke gate
(``tests/unit/test_mock_endpoint_smoke.py``): ``GET /api/deployments/diff`` was
permanently unreachable (routed to ``get_deployment_status("diff")`` → 404 "not
found (mock)") because ``deployment_diff.router`` was included AFTER
``deployments.router`` in ``main.py``. Fixed by moving it before, same as
``deployment_freshness``/``deployments_inventory`` above it.
"""

from __future__ import annotations


def _first_matching_endpoint_name(path: str, method: str = "GET") -> str:
    """The endpoint function name of the FIRST app route that fully matches path."""
    from unified_trading_library import find_matching_route

    from deployment_api.main import app

    route = find_matching_route(app, path, method)
    return route.endpoint.__name__ if route is not None else ""


def test_inventory_literal_not_shadowed_by_deployment_id() -> None:
    """/api/deployments/inventory resolves to the inventory handler, not status."""
    name = _first_matching_endpoint_name("/api/deployments/inventory")
    assert name == "get_deployment_inventory", (
        f"/api/deployments/inventory routed to {name!r} — the parametric "
        "/api/deployments/{deployment_id} is shadowing the literal inventory route "
        "(check include_router ordering in main.py)"
    )


def test_umbrella_summary_literal_not_shadowed() -> None:
    """/api/deployments/umbrella/{u}/summary resolves to the umbrella handler."""
    name = _first_matching_endpoint_name("/api/deployments/umbrella/LIVE/summary")
    assert name == "get_umbrella_summary", f"umbrella summary routed to {name!r}, not the umbrella handler"


def test_a_real_deployment_id_still_reaches_status_handler() -> None:
    """A non-literal deployment id still falls through to get_deployment_status."""
    name = _first_matching_endpoint_name("/api/deployments/some-real-deployment-id-123")
    assert name == "get_deployment_status", (
        f"a real deployment id routed to {name!r} — the parametric status route must "
        "still match non-literal ids after the inventory router was moved first"
    )


def test_diff_literal_not_shadowed_by_deployment_id() -> None:
    """/api/deployments/diff resolves to the diff handler, not status."""
    name = _first_matching_endpoint_name("/api/deployments/diff")
    assert name == "get_deployment_diff", (
        f"/api/deployments/diff routed to {name!r} — the parametric "
        "/api/deployments/{deployment_id} is shadowing the literal diff route "
        "(check include_router ordering in main.py)"
    )
