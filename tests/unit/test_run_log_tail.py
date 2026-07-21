"""Unit tests for routes/_run_log_tail.py — bounded byte-range tail + line-splitting.

Credential-free: ``gcs_read_object_range`` is mocked at the module's import site — no
real GCS access.
"""

from __future__ import annotations

from unittest.mock import patch

from deployment_api.routes._run_log_tail import read_run_log_tail, tail_lines_from_bytes


def test_tail_lines_from_bytes_drops_leading_partial_and_trailing_blank() -> None:
    """Mid-object read: the first fragment (cut mid-line) is dropped; trailing \\n produces no blank line."""
    raw = b"artial-line-remainder\nline2\nline3\n"
    result = tail_lines_from_bytes(raw, max_lines=10, drop_leading_partial=True)
    assert result == ["line2", "line3"]


def test_tail_lines_from_bytes_keeps_leading_fragment_when_read_starts_at_zero() -> None:
    raw = b"line1\nline2\nline3\n"
    result = tail_lines_from_bytes(raw, max_lines=10, drop_leading_partial=False)
    assert result == ["line1", "line2", "line3"]


def test_tail_lines_from_bytes_caps_to_max_lines() -> None:
    raw = b"a\nb\nc\nd\ne\n"
    result = tail_lines_from_bytes(raw, max_lines=2, drop_leading_partial=False)
    assert result == ["d", "e"]


def test_tail_lines_from_bytes_empty_input() -> None:
    assert tail_lines_from_bytes(b"", max_lines=10, drop_leading_partial=False) == []


def test_tail_lines_from_bytes_replaces_undecodable_bytes_instead_of_raising() -> None:
    """A byte-range read can cut a multi-byte UTF-8 sequence at the start boundary."""
    raw = b"\x80\x80invalid-lead\nclean line\n"
    result = tail_lines_from_bytes(raw, max_lines=10, drop_leading_partial=True)
    assert result == ["clean line"]


def test_read_run_log_tail_zero_size_never_calls_gcs() -> None:
    with patch("deployment_api.routes._run_log_tail.gcs_read_object_range") as mock_read:
        lines, tail_bytes = read_run_log_tail("gs://b/o", 0, max_bytes=1024, max_lines=10)
    assert lines == []
    assert tail_bytes == 0
    mock_read.assert_not_called()


def test_read_run_log_tail_small_object_reads_from_byte_zero() -> None:
    """size_bytes <= max_bytes: the whole object is within the cap, start=0, no partial drop."""
    with patch(
        "deployment_api.routes._run_log_tail.gcs_read_object_range",
        return_value=b"line1\nline2\n",
    ) as mock_read:
        lines, tail_bytes = read_run_log_tail("gs://b/o", 12, max_bytes=1024, max_lines=10)
    mock_read.assert_called_once_with("gs://b/o", 0, 12)
    assert lines == ["line1", "line2"]
    assert tail_bytes == 12


def test_read_run_log_tail_large_object_reads_only_the_capped_tail() -> None:
    """size_bytes > max_bytes: start = size - max_bytes, never the full object."""
    with patch(
        "deployment_api.routes._run_log_tail.gcs_read_object_range",
        return_value=b"-remainder\nlast line\n",
    ) as mock_read:
        lines, tail_bytes = read_run_log_tail("gs://b/o", 10_000_000, max_bytes=1024, max_lines=10)
    mock_read.assert_called_once_with("gs://b/o", 10_000_000 - 1024, 10_000_000)
    assert lines == ["last line"]
    assert tail_bytes == len(b"-remainder\nlast line\n")
