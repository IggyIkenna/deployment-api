"""Unit tests for the GitHub App installation-token rate-limit pool.

Credential-free: the aiohttp session is fully mocked (no network), the App JWT is
mocked, and the Secret Manager client is monkeypatched. Verifies:
  - gh_app_rate_limit mints an installation token (POST access_tokens) then reads the
    FREE /rate_limit with it, projecting {resources: {core, graphql}};
  - the `app` block is PRESENT when creds resolve and ABSENT when any cred is missing,
    and a mint/rate_limit failure never breaks the PAT measurement.
"""

from __future__ import annotations

from typing import cast

import pytest

import deployment_api.routes._repo_ci_github as gh


class _FakeResponse:
    """An async-context-manager standing in for an aiohttp response."""

    def __init__(self, status: int, json_body: object) -> None:
        self.status = status
        self._json_body = json_body

    async def __aenter__(self) -> _FakeResponse:
        return self

    async def __aexit__(self, *_: object) -> bool:
        return False

    async def json(self) -> object:
        return self._json_body

    async def text(self) -> str:
        return ""


class _FakeSession:
    """Replays scripted POST (token mint) + GET (/rate_limit) responses."""

    def __init__(self, post_responses: list[_FakeResponse], get_responses: list[_FakeResponse]) -> None:
        self._posts = post_responses
        self._gets = get_responses
        self._post_idx = 0
        self._get_idx = 0
        self.post_urls: list[str] = []
        self.get_urls: list[str] = []

    def post(self, url: str, headers: dict[str, str], timeout: object) -> _FakeResponse:
        self.post_urls.append(url)
        resp = self._posts[self._post_idx]
        self._post_idx += 1
        return resp

    def get(self, url: str, headers: dict[str, str], timeout: object) -> _FakeResponse:
        self.get_urls.append(url)
        resp = self._gets[self._get_idx]
        self._get_idx += 1
        return resp


@pytest.fixture(autouse=True)
def _reset_token_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gh, "_app_token_cache", None, raising=False)
    # The JWT mint is exercised separately; stub it so tests need no real PEM.
    monkeypatch.setattr(gh, "_make_app_jwt", lambda _app_id, _pem: "fake.jwt.token")


class TestGhAppRateLimit:
    @pytest.mark.asyncio
    async def test_mints_token_then_reads_free_rate_limit(self) -> None:
        token_resp = _FakeResponse(201, {"token": "ghs_app_token", "expires_at": "2026-06-10T13:00:00Z"})
        rate_resp = _FakeResponse(
            200,
            {
                "resources": {
                    "core": {"limit": 5000, "remaining": 4990, "used": 10, "reset": 1749560400},
                    "graphql": {"limit": 5000, "remaining": 5000, "used": 0, "reset": 1749560400},
                    "search": {"limit": 30, "remaining": 30, "used": 0, "reset": 1749556860},
                }
            },
        )
        fake = _FakeSession([token_resp], [rate_resp])
        session = cast(gh.aiohttp.ClientSession, fake)

        out = await gh.gh_app_rate_limit(session, "app123", "PEM", "inst456")

        assert out is not None
        resources = cast(dict[str, dict[str, int]], out["resources"])
        # Only core + graphql are projected for the App pool (search is PAT-only display).
        assert set(resources.keys()) == {"core", "graphql"}
        assert resources["core"] == {"limit": 5000, "remaining": 4990, "used": 10, "reset": 1749560400}
        # Minted via the installation access_tokens endpoint, then read the FREE rate_limit.
        assert fake.post_urls == [f"{gh._API_BASE}/app/installations/inst456/access_tokens"]
        assert fake.get_urls == [f"{gh._API_BASE}/rate_limit"]

    @pytest.mark.asyncio
    async def test_mint_failure_returns_none(self) -> None:
        fake = _FakeSession([_FakeResponse(403, {"message": "bad creds"})], [])
        session = cast(gh.aiohttp.ClientSession, fake)
        out = await gh.gh_app_rate_limit(session, "app123", "PEM", "inst456")
        assert out is None  # best-effort: no app block, never raises

    @pytest.mark.asyncio
    async def test_rate_limit_failure_returns_none(self) -> None:
        token_resp = _FakeResponse(201, {"token": "ghs_app_token", "expires_at": "2026-06-10T13:00:00Z"})
        fake = _FakeSession([token_resp], [_FakeResponse(500, {})])
        session = cast(gh.aiohttp.ClientSession, fake)
        out = await gh.gh_app_rate_limit(session, "app123", "PEM", "inst456")
        assert out is None


class _FakeSecretClient:
    def __init__(self, secrets: dict[str, str | None]) -> None:
        self._secrets = secrets

    def get_secret(self, secret_name: str, version: str = "latest", skip_cache: bool = False) -> str | None:
        return self._secrets.get(secret_name)


class TestResolveAppCredentials:
    @pytest.mark.asyncio
    async def test_all_present_returns_triple(self, monkeypatch: pytest.MonkeyPatch) -> None:
        secrets = {
            gh._APP_ID_SECRET: "app123",
            gh._APP_PRIVATE_KEY_SECRET: "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----",
            gh._APP_INSTALLATION_ID_SECRET: "inst456",
        }
        monkeypatch.setattr(gh, "get_secret_client", lambda project_id: _FakeSecretClient(secrets))
        got = await gh.resolve_app_credentials("proj")
        assert got is not None
        assert got[0] == "app123" and got[2] == "inst456"

    @pytest.mark.asyncio
    async def test_missing_one_cred_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        secrets: dict[str, str | None] = {
            gh._APP_ID_SECRET: "app123",
            gh._APP_PRIVATE_KEY_SECRET: None,  # absent
            gh._APP_INSTALLATION_ID_SECRET: "inst456",
        }
        monkeypatch.setattr(gh, "get_secret_client", lambda project_id: _FakeSecretClient(secrets))
        got = await gh.resolve_app_credentials("proj")
        assert got is None  # App pool simply not surfaced
