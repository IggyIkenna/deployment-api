"""Unit tests for the GitHub REST helper ETag conditional cache + free /rate_limit.

Credential-free: the aiohttp session is fully mocked, no network. Verifies:
  - a 304 Not Modified serves the cached body (the FREE conditional path), and
    the prior 200's ETag is echoed back as `If-None-Match`;
  - gh_rate_limit parses /rate_limit into the exact UI shape (never cached/conditional).
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
    """Replays a scripted list of responses and records the request headers seen."""

    def __init__(self, responses: list[_FakeResponse]) -> None:
        self._responses = responses
        self._idx = 0
        self.seen_headers: list[dict[str, str]] = []

    def get(self, url: str, headers: dict[str, str], timeout: object) -> _FakeResponse:
        self.seen_headers.append(dict(headers))
        resp = self._responses[self._idx]
        self._idx += 1
        return resp


@pytest.fixture(autouse=True)
def _clear_cache() -> None:
    gh._response_cache.clear()


def _session(responses: list[_FakeResponse]) -> gh.aiohttp.ClientSession:
    return cast(gh.aiohttp.ClientSession, _FakeSession(responses))


class TestEtagConditional:
    @pytest.mark.asyncio
    async def test_304_serves_cached_body_for_free(self) -> None:
        body = {"sha": "abc", "value": 1}
        # First call: a 200 with an ETag populates body + etag in the cache.
        first = _FakeResponse(200, body, {"ETag": 'W/"deadbeef"'})
        # Second call (after TTL lapse): GitHub answers 304 — body must come from cache.
        second = _FakeResponse(304, None, {})
        fake = _FakeSession([first, second])
        session = cast(gh.aiohttp.ClientSession, fake)
        token = "t"

        got1 = await gh.gh_get_json(session, token, "/repos/o/r/commits/abc")
        assert got1 == body

        # Expire the TTL so the second call goes to the network with If-None-Match.
        url = f"{gh._API_BASE}/repos/o/r/commits/abc"
        ts, payload, etag = gh._response_cache[url]
        gh._response_cache[url] = (ts - (gh._CACHE_TTL_SECONDS + 1.0), payload, etag)

        got2 = await gh.gh_get_json(session, token, "/repos/o/r/commits/abc")
        assert got2 == body  # served from cache on 304
        # The conditional header carried the prior ETag.
        assert fake.seen_headers[1].get("If-None-Match") == 'W/"deadbeef"'

    @pytest.mark.asyncio
    async def test_fresh_ttl_hit_makes_no_request(self) -> None:
        body = {"sha": "z"}
        fake = _FakeSession([_FakeResponse(200, body, {"ETag": '"e1"'})])
        session = cast(gh.aiohttp.ClientSession, fake)

        got1 = await gh.gh_get_json(session, "t", "/repos/o/r/x")
        got2 = await gh.gh_get_json(session, "t", "/repos/o/r/x")
        assert got1 == body and got2 == body
        # Only ONE network call — the second is a fresh-TTL cache hit.
        assert len(fake.seen_headers) == 1

    @pytest.mark.asyncio
    async def test_200_refreshes_etag(self) -> None:
        fake = _FakeSession(
            [
                _FakeResponse(200, {"v": 1}, {"ETag": '"old"'}),
                _FakeResponse(200, {"v": 2}, {"ETag": '"new"'}),
            ]
        )
        session = cast(gh.aiohttp.ClientSession, fake)
        await gh.gh_get_json(session, "t", "/p")
        url = f"{gh._API_BASE}/p"
        ts, payload, etag = gh._response_cache[url]
        gh._response_cache[url] = (ts - (gh._CACHE_TTL_SECONDS + 1.0), payload, etag)
        got2 = await gh.gh_get_json(session, "t", "/p")
        assert got2 == {"v": 2}
        # Second request sent the first ETag; cache now holds the refreshed one.
        assert fake.seen_headers[1].get("If-None-Match") == '"old"'
        assert gh._response_cache[url][2] == '"new"'


class TestGhRateLimit:
    @pytest.mark.asyncio
    async def test_parses_resources_and_never_conditional(self) -> None:
        payload = {
            "resources": {
                "core": {"limit": 5000, "remaining": 4000, "used": 1000, "reset": 1749560400},
                "graphql": {"limit": 5000, "remaining": 5000, "used": 0, "reset": 1749560400},
                "search": {"limit": 30, "remaining": 28, "used": 2, "reset": 1749556860},
            }
        }
        fake = _FakeSession([_FakeResponse(200, payload, {"ETag": '"rl"'})])
        session = cast(gh.aiohttp.ClientSession, fake)

        out = await gh.gh_rate_limit(session, "t")

        assert set(out.keys()) == {"fetched_at", "resources"}
        resources = cast(dict[str, dict[str, int]], out["resources"])
        assert set(resources.keys()) == {"core", "graphql", "search"}
        assert resources["core"] == {"limit": 5000, "remaining": 4000, "used": 1000, "reset": 1749560400}
        assert resources["search"]["remaining"] == 28
        # ISO minute UTC shape: YYYY-MM-DDTHH:MMZ
        fetched = cast(str, out["fetched_at"])
        assert fetched.endswith("Z") and fetched[10] == "T" and len(fetched) == 17
        # /rate_limit is FREE — no If-None-Match ever sent.
        assert "If-None-Match" not in fake.seen_headers[0]

    @pytest.mark.asyncio
    async def test_missing_blocks_default_to_zero(self) -> None:
        fake = _FakeSession([_FakeResponse(200, {"resources": {}}, {})])
        session = cast(gh.aiohttp.ClientSession, fake)
        out = await gh.gh_rate_limit(session, "t")
        resources = cast(dict[str, dict[str, int]], out["resources"])
        assert resources["core"] == {"limit": 0, "remaining": 0, "used": 0, "reset": 0}
