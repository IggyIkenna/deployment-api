"""Unit tests for deployment_api/utils/request_memory_profiling.py.

Mirrors test_bounded_subprocess.py's style: patch the module-level `resource` name so
tests are deterministic and never depend on the actual process RSS.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

from deployment_api.utils.request_memory_profiling import (
    _WARN_THRESHOLD_KIB,
    log_rss_delta,
)

_PATCH_RESOURCE = "deployment_api.utils.request_memory_profiling.resource"
_LOGGER_NAME = "deployment_api.utils.request_memory_profiling"


def _rusage_with(maxrss: int) -> MagicMock:
    rusage = MagicMock()
    rusage.ru_maxrss = maxrss
    return rusage


class TestLogRssDelta:
    def test_small_delta_logs_at_debug(self, caplog):
        with patch(_PATCH_RESOURCE) as mock_resource:
            mock_resource.getrusage.side_effect = [
                _rusage_with(100_000),
                _rusage_with(100_500),  # 500 KiB delta — below the warn threshold
            ]
            with caplog.at_level(logging.DEBUG, logger=_LOGGER_NAME):
                with log_rss_delta("some.handler"):
                    pass

        records = [r for r in caplog.records if r.name == _LOGGER_NAME]
        assert len(records) == 1
        assert records[0].levelno == logging.DEBUG
        assert "some.handler" in records[0].getMessage()
        assert "peak_rss_delta_kib=500" in records[0].getMessage()

    def test_large_delta_logs_at_warning(self, caplog):
        start = 100_000
        end = start + _WARN_THRESHOLD_KIB + 1
        with patch(_PATCH_RESOURCE) as mock_resource:
            mock_resource.getrusage.side_effect = [_rusage_with(start), _rusage_with(end)]
            with caplog.at_level(logging.DEBUG, logger=_LOGGER_NAME):
                with log_rss_delta("heavy.handler"):
                    pass

        records = [r for r in caplog.records if r.name == _LOGGER_NAME]
        assert len(records) == 1
        assert records[0].levelno == logging.WARNING
        assert "heavy.handler" in records[0].getMessage()

    def test_logs_even_when_body_raises(self, caplog):
        with patch(_PATCH_RESOURCE) as mock_resource:
            mock_resource.getrusage.side_effect = [_rusage_with(1_000), _rusage_with(1_100)]
            with caplog.at_level(logging.DEBUG, logger=_LOGGER_NAME):
                try:
                    with log_rss_delta("erroring.handler"):
                        raise ValueError("boom")
                except ValueError:
                    pass

        records = [r for r in caplog.records if r.name == _LOGGER_NAME]
        assert len(records) == 1
        assert "erroring.handler" in records[0].getMessage()
