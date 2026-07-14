"""Unit tests for the cost-observability AWS Athena WIF reader (aws_wif.py).

Mirrors tests/unit/test_code_builds_aws.py's TestWifCodebuildClient — same keyless GCP->AWS WIF
flow, applied to the distinct Athena reader role. No real AWS/Google calls (boto3 + google.auth
are mocked); `_wif_creds_cache` is reset around each test so the ~50min cache doesn't leak state
across tests.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from deployment_api.services.cost_observability.aws_wif import (
    AWSAnalyticsClient,
    _WIFAWSAnalyticsClient,
    get_athena_analytics_client,
)


def _reset_cache() -> None:
    import deployment_api.services.cost_observability.aws_wif as mod

    mod._wif_creds_cache = None  # pyright: ignore[reportPrivateUsage]


class TestGetAthenaAnalyticsClient:
    """The factory: empty role ARN -> plain (today's) client; set -> the WIF-credentialed subclass."""

    def test_no_role_arn_returns_plain_client(self) -> None:
        client = get_athena_analytics_client(region="us-east-1", output_bucket="uts-billing-cur", role_arn="")
        assert type(client) is AWSAnalyticsClient  # exact-type check (not just isinstance) — not the WIF subclass
        assert not isinstance(client, _WIFAWSAnalyticsClient)

    def test_role_arn_set_returns_wif_client(self) -> None:
        role_arn = "arn:aws:iam::427895769566:role/gcp-cloudrun-athena-reader"
        client = get_athena_analytics_client(region="us-east-1", output_bucket="uts-billing-cur", role_arn=role_arn)
        assert isinstance(client, _WIFAWSAnalyticsClient)
        assert client._role_arn == role_arn  # pyright: ignore[reportPrivateUsage]


class TestWifAthenaBoto3Client:
    """`_WIFAWSAnalyticsClient._boto3_client` — the single AWS-SDK-boundary override."""

    def test_role_arn_set_assumes_role_keyless(self) -> None:
        _reset_cache()
        fake_sts = MagicMock()
        fake_sts.assume_role_with_web_identity.return_value = {
            "Credentials": {
                "AccessKeyId": "AKIA_TMP",
                "SecretAccessKey": "SECRET_TMP",
                "SessionToken": "TOKEN_TMP",
            }
        }
        fake_athena = MagicMock()
        fake_session = MagicMock()
        fake_session.client.return_value = fake_athena
        role_arn = "arn:aws:iam::427895769566:role/gcp-cloudrun-athena-reader"

        client = _WIFAWSAnalyticsClient(region="us-east-1", output_bucket="uts-billing-cur", role_arn=role_arn)
        with (
            patch("boto3.client", return_value=fake_sts) as boto_client,
            patch("boto3.Session", return_value=fake_session) as session_ctor,
            patch("google.oauth2.id_token.fetch_id_token", return_value="google-oidc-token"),
            patch("google.auth.transport.requests.Request", return_value=MagicMock()),
        ):
            aws_client = client._boto3_client("athena")  # pyright: ignore[reportPrivateUsage]
            _reset_cache()

        # STS client built for the unsigned web-identity call; the role assumed with the OIDC token.
        assert boto_client.call_args.args[0] == "sts"
        fake_sts.assume_role_with_web_identity.assert_called_once()
        kw = fake_sts.assume_role_with_web_identity.call_args.kwargs
        assert kw["RoleArn"] == role_arn
        assert kw["WebIdentityToken"] == "google-oidc-token"
        assert kw["RoleSessionName"] == "cost-observability-athena-reader"
        # Session built from the SHORT-LIVED assumed creds — never a stored static key.
        session_ctor.assert_called_once_with(
            region_name="us-east-1",
            aws_access_key_id="AKIA_TMP",
            aws_secret_access_key="SECRET_TMP",
            aws_session_token="TOKEN_TMP",
        )
        fake_session.client.assert_called_once_with("athena")
        assert aws_client is fake_athena

    def test_no_role_arn_uses_default_credential_chain(self) -> None:
        _reset_cache()
        fake_athena = MagicMock()
        client = get_athena_analytics_client(region="us-east-1", output_bucket="uts-billing-cur", role_arn="")
        with (
            patch("boto3.Session") as session_ctor,
        ):
            session_ctor.return_value.client.return_value = fake_athena
            aws_client = client._boto3_client("athena")  # pyright: ignore[reportPrivateUsage]

        # Plain AWSAnalyticsClient (unpatched, upstream UTL behaviour) — no WIF session assembly here.
        session_ctor.assert_called_once()
        assert "aws_access_key_id" not in session_ctor.call_args.kwargs
        assert aws_client is fake_athena

    def test_wif_creds_cached_no_reassume_on_second_call(self) -> None:
        _reset_cache()
        fake_sts = MagicMock()
        fake_sts.assume_role_with_web_identity.return_value = {
            "Credentials": {"AccessKeyId": "A", "SecretAccessKey": "S", "SessionToken": "T"}
        }
        client = _WIFAWSAnalyticsClient(
            region="us-east-1", output_bucket="uts-billing-cur", role_arn="arn:aws:iam::1:role/r"
        )
        with (
            patch("boto3.client", return_value=fake_sts),
            patch("boto3.Session", return_value=MagicMock()),
            patch("google.oauth2.id_token.fetch_id_token", return_value="tok"),
            patch("google.auth.transport.requests.Request", return_value=MagicMock()),
        ):
            client._boto3_client("athena")  # pyright: ignore[reportPrivateUsage]
            client._boto3_client("glue")  # pyright: ignore[reportPrivateUsage]
            _reset_cache()

        # The role is assumed ONCE; the second (different-service) client reuses the cached creds.
        assert fake_sts.assume_role_with_web_identity.call_count == 1
