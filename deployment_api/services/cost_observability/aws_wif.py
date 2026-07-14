"""Keyless GCP->AWS Workload-Identity-Federation for the cost-observability AWS Athena reader.

The deployed Cloud Run container has NO ambient AWS credentials at all (verified live — its only
env vars are GCP-side: GCP_PROJECT_ID/CLOUD_PROVIDER/CLOUD_MOCK_MODE/DEPLOYMENT_ENV/DISABLE_AUTH),
so a bare ``AWSAnalyticsClient`` (default boto3 credential chain) silently returns nothing there —
it only works locally, where a developer's AWS profile/env happens to be present. This module
mirrors ``deployment_api.routes._code_builds_aws._assume_codebuild_reader_role`` (the SAME flow,
proven live for the AWS CodeBuild reader) for a distinct role (``aws_athena_reader_role_arn``)
scoped to Athena/Glue instead of CodeBuild: (1) mint a Google OIDC ID token for the Cloud Run
service account from the metadata server; (2) exchange it for short-lived STS creds via
``AssumeRoleWithWebIdentity`` against the configured role ARN (the role's trust policy is locked to
this SA's OIDC subject + grants read-only Athena/Glue). No static AWS key anywhere.

Inert by default: ``get_athena_analytics_client`` returns a plain ``AWSAnalyticsClient`` (today's
behaviour — the default boto3 credential chain) whenever no role ARN is configured, so this is a
complete no-op — zero risk to the currently-working GCP/GitHub cost tabs — until an operator
provisions the AWS-side role (out of scope here; no IAM/trust-policy change ships with this module)
and sets ``AWS_ATHENA_READER_ROLE_ARN``.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, cast

from unified_trading_library.cloud_interface.providers.aws import (  # noqa: qg-deep-import
    AWSAnalyticsClient,
)

if TYPE_CHECKING:
    from typing import Any

    # boto3 clients don't have complete typing at this boundary — same convention as
    # deployment_api.routes._code_builds_aws.
    STSClient = Any
else:
    STSClient = object

# Assumed-role creds are valid 1h; refresh at 50min so a request never races the expiry.
# Module-global (process-wide) — the reader is read-only + idempotent, same caching contract as
# the CodeBuild reader's own cache (a distinct cache: a distinct role, a distinct trust policy).
_WIF_CACHE_TTL_SECONDS = 50 * 60
_wif_creds_cache: tuple[float, dict[str, str]] | None = None


def _assume_athena_reader_role(role_arn: str, region: str) -> dict[str, str]:
    """Mint short-lived AWS creds via keyless GCP->AWS WIF against ``role_arn``.

    Returns boto3 client kwargs (``aws_access_key_id``/``aws_secret_access_key``/
    ``aws_session_token``); cached ~50min. Same flow as
    ``_code_builds_aws._assume_codebuild_reader_role``, just parameterized on the role ARN + region
    instead of hardcoding the CodeBuild reader's — the Athena reader role is a distinct role with
    its own trust policy + permission set.
    """
    global _wif_creds_cache
    now = time.monotonic()
    if _wif_creds_cache is not None and now < _wif_creds_cache[0]:
        return _wif_creds_cache[1]

    import boto3  # Deferred — AWS SDK boundary
    import google.auth.transport.requests  # Deferred — Google auth boundary
    import google.oauth2.id_token  # Deferred — Google auth boundary

    # The role trust conditions only on the SA's OIDC subject, so any stable audience works; the
    # role ARN is a convenient, self-documenting choice (matches the CodeBuild reader's approach).
    id_token: str = cast(
        str,
        google.oauth2.id_token.fetch_id_token(  # type: ignore[reportUnknownMemberType]
            google.auth.transport.requests.Request(), role_arn
        ),
    )
    # AssumeRoleWithWebIdentity needs NO existing AWS credentials (the web-identity token IS the
    # auth), so a plain STS client works even on a host with no AWS creds (the GCP-hosted dashboard).
    sts: STSClient = boto3.client("sts", region_name=region)  # type: ignore[reportUnknownMemberType, reportAny]
    resp: dict[str, object] = cast(
        dict[str, object],
        sts.assume_role_with_web_identity(  # type: ignore[reportUnknownMemberType]
            RoleArn=role_arn,
            RoleSessionName="cost-observability-athena-reader",
            WebIdentityToken=id_token,
        ),
    )
    creds: dict[str, object] = cast(dict[str, object], resp["Credentials"])
    out = {
        "aws_access_key_id": str(creds["AccessKeyId"]),
        "aws_secret_access_key": str(creds["SecretAccessKey"]),
        "aws_session_token": str(creds["SessionToken"]),
    }
    _wif_creds_cache = (now + _WIF_CACHE_TTL_SECONDS, out)
    return out


class _WIFAWSAnalyticsClient(AWSAnalyticsClient):
    """``AWSAnalyticsClient`` whose boto3 session carries WIF-assumed-role creds.

    Overrides ``_boto3_client`` — the client's own single AWS-SDK-boundary method (used for both
    its Athena `execute_query` and Glue calls) — so every AWS SDK call this client makes goes
    through the assumed-role session instead of a bare ``boto3.Session()`` default credential
    chain. No change to ``unified-trading-library`` itself: UTL's role here is a generic
    default-credential-chain client, and swapping its credential source is exactly the extension
    point ``_boto3_client`` (private, but per-instance overridable) exists for.
    """

    def __init__(self, region: str, output_bucket: str, role_arn: str) -> None:
        super().__init__(region=region, output_bucket=output_bucket)
        self._role_arn = role_arn

    def _boto3_client(self, service: str) -> object:
        import boto3  # Deferred — AWS SDK boundary

        creds = _assume_athena_reader_role(self._role_arn, self._region)
        session = boto3.Session(region_name=self._region, **creds)  # type: ignore[reportUnknownMemberType, reportAny]
        client_fn = session.client  # type: ignore[reportUnknownMemberType, reportAny]
        return client_fn(service)  # type: ignore[reportUnknownVariableType, reportAny]


def get_athena_analytics_client(region: str, output_bucket: str, role_arn: str) -> AWSAnalyticsClient:
    """AWS Athena analytics client for the cost-observability CUR query.

    When ``role_arn`` is set (post AWS-side role provisioning), auth is keyless GCP->AWS WIF —
    short-lived assumed-role creds, no static AWS key. When unset (today's default / a native-AWS
    deployment / local AWS profile), the default boto3 credential chain is used — identical to the
    pre-existing ``AWSAnalyticsClient(region=region, output_bucket=output_bucket)`` call site this
    replaces, so this is a no-op until an operator sets the role ARN.
    """
    if role_arn:
        return _WIFAWSAnalyticsClient(region=region, output_bucket=output_bucket, role_arn=role_arn)
    return AWSAnalyticsClient(region=region, output_bucket=output_bucket)
