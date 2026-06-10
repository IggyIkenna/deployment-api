"""
Pytest configuration and fixtures for deployment-api tests.
"""

import socket

import pytest

from tests.mocks import make_mock_path_combinatorics

_original_connect = socket.socket.connect


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--block-network",
        action="store_true",
        default=False,
        help="Block all socket connections to enforce credential-free CI runs.",
    )


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "allow_network: mark test as allowed to make network calls (opt-out of --block-network)",
    )


def _blocked_connect(self: socket.socket, address: object) -> None:
    raise OSError(f"Network access blocked by --block-network: attempted connection to {address}")


@pytest.fixture(autouse=True)
def _enforce_block_network(request: pytest.FixtureRequest) -> object:
    if not request.config.getoption("--block-network", default=False):
        yield
        return
    if request.node.get_closest_marker("allow_network"):
        yield
        return
    socket.socket.connect = _blocked_connect  # type: ignore[method-assign]
    yield
    socket.socket.connect = _original_connect  # type: ignore[method-assign]


@pytest.fixture(autouse=True)
def _ensure_events_initialized() -> None:
    """Guarantee UTL event logging is initialized before every test.

    `log_event()` hard-raises ``RuntimeError: Event logging not initialized`` when
    the UTL events global is unset. Several tests reconfigure or tear down event
    logging (app-lifespan exercises, mode switches), leaving the global cleared for
    whatever test pytest runs next — so unrelated tests that call ``log_event``
    (e.g. ``tarball_staleness.trigger_refresh``, ``vm_events._parse_event_jsonl``)
    fail purely on collection ORDER. Re-initializing per test makes the suite
    order-independent. ``setup_events`` is idempotent, so this is cheap and safe for
    tests that set up their own event state in-body.
    """
    from unified_trading_library import setup_events

    setup_events("deployment-api", "test")


@pytest.fixture
def mock_path_combinatorics():
    """Return a disabled PathCombinatorics mock (forces directory-listing path).

    Use in data_status tests to bypass combinatorics and exercise GCS listing.
    """
    return make_mock_path_combinatorics()
