"""Regression tests for the stdout/stderr Cloud Logging blackout fix.

plans/active/issues/deployment_api_sigabrt_crash_loop_2026_07_24.md's "blackout" todo: a bare
``logging.basicConfig(level=logging.INFO)`` emits plain, unstructured text with no recognizable
severity. Cloud Run's log ingestion stamps any non-JSON-structured stdout/stderr line with
``severity=DEFAULT`` (0), and this project's ``_Default`` Cloud Logging sink excludes
``severity <= "DEBUG"`` (100) for cost control — so every plain-text log line from this service was
silently discarded sink-side, confirmed via 4 live zero-traffic Cloud Run canary deploys (a bare
unstructured stdout write never appeared; a structured ``{"severity": "INFO", ...}`` JSON line
did). The fix
switches to ``setup_cloud_logging()``, whose ``CloudRunJSONFormatter`` emits GCP-recognized
structured JSON with an explicit ``severity`` field that survives the exclusion at INFO and above.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path

import pytest
from unified_trading_library import setup_cloud_logging

_MAIN_PY = Path(__file__).resolve().parents[2] / "deployment_api" / "main.py"


def test_main_py_does_not_call_bare_basic_config() -> None:
    """Regression guard: a bare `logging.basicConfig(...)` re-introduces the blackout (its default
    formatter has no `severity` field, so Cloud Run stamps `DEFAULT` severity and the project's
    `_Default` sink's `severity <= "DEBUG"` exclusion silently drops every line)."""
    text = _MAIN_PY.read_text(encoding="utf-8")
    assert not re.search(r"^\s*logging\.basicConfig\(", text, re.MULTILINE), (
        "deployment_api/main.py must not call logging.basicConfig() directly — "
        "use unified_trading_library.setup_cloud_logging() instead (structured JSON severity "
        "survives the project's severity<=DEBUG Cloud Logging exclusion; plain text does not)"
    )


def test_main_py_calls_setup_cloud_logging() -> None:
    """main.py must bootstrap logging via the structured-JSON helper."""
    text = _MAIN_PY.read_text(encoding="utf-8")
    assert "setup_cloud_logging(" in text
    assert "from unified_trading_library import" in text
    assert "setup_cloud_logging" in text.split("from unified_trading_library import", 1)[1].split(")", 1)[0]


def test_setup_cloud_logging_emits_json_with_explicit_severity(capsys: pytest.CaptureFixture[str]) -> None:
    """Functional proof the emitted line is GCP-structured-logging-compatible: valid JSON with an
    explicit `severity` field at INFO (200) or above — i.e. NOT `<= DEBUG` (100), the exact
    project-wide Cloud Logging exclusion threshold this fix is designed to survive."""
    import sys

    logger = setup_cloud_logging(log_level="INFO", json_format=True)
    try:
        logger.info("regression-test log line")
        captured = capsys.readouterr()
        line = captured.err.strip().splitlines()[-1]
        payload = json.loads(line)
        assert payload["severity"] == "INFO"
        assert payload["message"] == "regression-test log line"
    finally:
        logger.handlers.clear()
        logging.getLogger().setLevel(logging.WARNING)
        sys.stderr.flush()
