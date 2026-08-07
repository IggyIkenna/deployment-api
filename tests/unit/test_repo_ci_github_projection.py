"""Unit tests for the response-projection memory trim in the repo-CI GitHub client.

``deployment_api_sigabrt_crash_loop_2026_07_24.md``'s [BACKEND] P2 (2026-08-06) traced the
cockpit rollup handlers' multi-GiB single-call peaks to the 90 s response cache retaining
FULL GitHub bodies (measured compare payload 1.3 MB for unified-trading-pm / 1.1 MB for
agent-orchestrator, full file patches) when the overview reads only a few fields. The
``gh_get_json(..., project=...)`` parameter reduces each payload BEFORE it is returned AND
cached, so the retained shape is the slimmed slice.

Credential-free: the aiohttp session is fully mocked, no network. Verifies:
  - compare_branches / diverged_content_lag read their slices off the PROJECTED compare shape,
    and the response cache stores the reduced body (no file patches);
  - list_open_promotion_prs classifies PRs off the projected pulls list + detail;
  - head_check_rollup reads name/path/conclusion off the projected runs list;
  - gh_get_json with a projector stores the projected value in the cache.
"""

from __future__ import annotations

from typing import cast

import pytest

import deployment_api.routes._repo_ci_github as gh


class _FakeResponse:
    """An async-context-manager standing in for an aiohttp response."""

    def __init__(self, status: int, json_body: object, headers: dict[str, str]) -> None:
        self.status = status
        self._json_body = json_body
        self.headers = headers

    async def __aenter__(self) -> _FakeResponse:
        return self

    async def __aexit__(self, *_: object) -> bool:
        return False

    async def json(self) -> object:
        return self._json_body

    async def text(self) -> str:
        return ""


class _FakeSession:
    """Replays a scripted list of responses in order and records the request paths seen."""

    def __init__(self, responses: list[_FakeResponse]) -> None:
        self._responses = responses
        self._idx = 0
        self.seen_paths: list[str] = []

    def get(self, url: str, headers: dict[str, str], timeout: object) -> _FakeResponse:
        self.seen_paths.append(url)
        resp = self._responses[self._idx]
        self._idx += 1
        return resp


@pytest.fixture(autouse=True)
def _clear_cache() -> None:
    gh._response_cache.clear()


def _session(responses: list[_FakeResponse]) -> gh.aiohttp.ClientSession:
    return cast(gh.aiohttp.ClientSession, _FakeSession(responses))


def _cached_body(path: str) -> object:
    """The parsed body currently retained in the response cache for a path (None if not cached)."""
    url = f"{gh._API_BASE}{path}"
    entry = gh._response_cache.get(url)
    return entry[1] if entry is not None else None


class TestCompareProjection:
    @pytest.mark.asyncio
    async def test_compare_branches_reads_projected_slice_and_caches_reduced_body(self) -> None:
        # A raw compare body with file patches (the multi-MB case).
        raw = {
            "ahead_by": 12,
            "behind_by": 3,
            "total_files_changed": 2,
            "status": "diverged",
            "commits": [{"sha": "c1"}],
            "files": [
                {"filename": "a.py", "status": "modified", "additions": 5, "deletions": 1, "patch": "+x\n-x\n"},
                {"filename": "b.py", "status": "added", "additions": 9, "patch": "+y\n"},
            ],
        }
        fake = _FakeSession([_FakeResponse(200, raw, {"ETag": '"c1"'})])
        session = cast(gh.aiohttp.ClientSession, fake)
        path = "/repos/IggyIkenna/r/compare/main...live-defi-rollout?per_page=1"

        got = await gh.compare_branches(session, "t", "IggyIkenna", "r", "main", "live-defi-rollout")

        assert got == (12, 3, 2)
        cached = cast("dict[str, object]", _cached_body(path))
        # The retained shape is the projected slice: no patch/status/additions/deletions, files
        # reduced to filenames only.
        assert set(cached.keys()) == {"ahead_by", "behind_by", "total_files_changed", "files"}
        assert cached["files"] == [{"filename": "a.py"}, {"filename": "b.py"}]
        assert "commits" not in cached and "status" not in cached

    @pytest.mark.asyncio
    async def test_diverged_content_lag_reads_filenames_off_projected_compare(self) -> None:
        compare_raw = {
            "ahead_by": 5,
            "behind_by": 0,
            "total_files_changed": 2,
            "files": [{"filename": "x.py", "patch": "+.."}, {"filename": "y.py", "patch": "-.."}],
        }
        commit_rows = [{"sha": "s1", "commit": {"committer": {"date": "2026-08-01T10:00:00Z"}}}]
        commit_rows_2 = [{"sha": "s2", "commit": {"committer": {"date": "2026-08-01T09:00:00Z"}}}]
        fake = _FakeSession(
            [
                _FakeResponse(200, compare_raw, {"ETag": '"cmp"'}),
                _FakeResponse(200, commit_rows, {"ETag": '"cr1"'}),
                _FakeResponse(200, commit_rows_2, {"ETag": '"cr2"'}),
            ]
        )
        session = cast(gh.aiohttp.ClientSession, fake)

        oldest, count = await gh.diverged_content_lag(session, "t", "IggyIkenna", "r", "main", "live-defi-rollout")

        assert oldest == "2026-08-01T09:00:00Z"  # the OLDEST of the two file last-set times
        assert count == 2  # two distinct shas
        assert len(fake.seen_paths) == 3  # one projected compare + one per-file last-set query

    @pytest.mark.asyncio
    async def test_absent_compare_returns_none(self) -> None:
        fake = _FakeSession([_FakeResponse(404, None, {})])
        session = cast(gh.aiohttp.ClientSession, fake)
        got = await gh.compare_branches(session, "t", "IggyIkenna", "r", "staging", "main")
        assert got is None


class TestPullsProjection:
    @pytest.mark.asyncio
    async def test_open_promotion_prs_classify_off_projected_list_and_detail(self) -> None:
        # List payload: one promotion PR into main, one PR into a non-promotion branch, one draft.
        list_raw = [
            {
                "number": 42,
                "title": "promote LDR",
                "html_url": "https://github.com/IggyIkenna/r/pull/42",
                "created_at": "2026-08-01T10:00:00Z",
                "auto_merge": None,
                "mergeable_state": "clean",
                "draft": False,
                "base": {"ref": "main"},
                "head": {"ref": "live-defi-rollout", "sha": "abc123"},
            },
            {
                "number": 43,
                "title": "unrelated",
                "html_url": "https://github.com/IggyIkenna/r/pull/43",
                "created_at": "2026-08-01T10:00:00Z",
                "auto_merge": None,
                "mergeable_state": "clean",
                "draft": False,
                "base": {"ref": "develop"},
                "head": {"ref": "feature/x"},
            },
            {
                "number": 44,
                "title": "draft PR",
                "html_url": "https://github.com/IggyIkenna/r/pull/44",
                "created_at": "2026-08-01T10:00:00Z",
                "auto_merge": None,
                "mergeable_state": "clean",
                "draft": True,
                "base": {"ref": "main"},
                "head": {"ref": "live-defi-rollout"},
            },
        ]
        detail_raw = {
            "number": 42,
            "title": "promote LDR",
            "html_url": "https://github.com/IggyIkenna/r/pull/42",
            "created_at": "2026-08-01T10:00:00Z",
            "auto_merge": None,
            "mergeable_state": "clean",
            "draft": False,
            "base": {"ref": "main"},
            "head": {"ref": "live-defi-rollout", "sha": "abc123"},
        }
        fake = _FakeSession(
            [
                _FakeResponse(200, list_raw, {"ETag": '"pl1"'}),
                _FakeResponse(200, detail_raw, {"ETag": '"pd1"'}),
            ]
        )
        session = cast(gh.aiohttp.ClientSession, fake)

        prs = await gh.list_open_promotion_prs(session, "t", "IggyIkenna", "r")

        # Only the promotion PR survives classification; the enriched shape is unchanged.
        assert len(prs) == 1
        assert prs[0]["number"] == 42
        assert prs[0]["head"] == "live-defi-rollout"
        assert prs[0]["base"] == "main"
        assert prs[0]["head_sha"] == "abc123"
        assert prs[0]["mergeable_state"] == "clean"
        assert prs[0]["auto_merge"] is False
        # The cached list holds the projected, patch-free shape.
        cached = _cached_body("/repos/IggyIkenna/r/pulls?state=open&per_page=30")
        assert isinstance(cached, list)
        assert set(cast("dict[str, object]", cached[0]).keys()) == {
            "number",
            "title",
            "html_url",
            "created_at",
            "auto_merge",
            "mergeable_state",
            "draft",
            "base",
            "head",
        }
        assert cast("dict[str, object]", cached[0])["head"] == {"ref": "live-defi-rollout", "sha": "abc123"}


class TestWorkflowRunsProjection:
    @pytest.mark.asyncio
    async def test_head_check_rollup_reads_projected_runs(self) -> None:
        runs_raw = {
            "workflow_runs": [
                {"name": "quality-gates-v2", "path": ".github/workflows/quality-gates-v2.yml", "conclusion": "failure"},
                {"name": "notify", "path": ".github/workflows/notify.yml", "conclusion": "success"},
            ],
            "total_count": 2,
        }
        fake = _FakeSession([_FakeResponse(200, runs_raw, {"ETag": '"wr1"'})])
        session = cast(gh.aiohttp.ClientSession, fake)
        path = "/repos/IggyIkenna/r/actions/runs?head_sha=abc123&per_page=100"

        failed, v2_present = await gh.head_check_rollup(session, "t", "IggyIkenna", "r", "abc123")

        assert failed is True  # quality-gates-v2 concluded failure
        assert v2_present is True  # the workflow path carries quality-gates-v2
        cached = cast("dict[str, object]", _cached_body(path))
        assert "total_count" not in cached
        assert cached["workflow_runs"] == [
            {"name": "quality-gates-v2", "path": ".github/workflows/quality-gates-v2.yml", "conclusion": "failure"},
            {"name": "notify", "path": ".github/workflows/notify.yml", "conclusion": "success"},
        ]

    @pytest.mark.asyncio
    async def test_projector_stores_projected_value_in_cache(self) -> None:
        raw = {"a": 1, "b": {"deep": [1, 2, 3]}}
        fake = _FakeSession([_FakeResponse(200, raw, {"ETag": '"p1"'})])
        session = cast(gh.aiohttp.ClientSession, fake)
        path = "/repos/IggyIkenna/r/x"

        got = await gh.gh_get_json(session, "t", path, project=lambda payload: {"a": 1})

        assert got == {"a": 1}
        assert _cached_body(path) == {"a": 1}
