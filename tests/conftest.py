"""
Pytest configuration and fixtures for deployment-api tests.
"""

import pytest

from tests.mocks import make_mock_path_combinatorics


@pytest.fixture
def mock_path_combinatorics():
    """Return a disabled PathCombinatorics mock (forces directory-listing path).

    Use in data_status tests to bypass combinatorics and exercise GCS listing.
    """
    return make_mock_path_combinatorics()
