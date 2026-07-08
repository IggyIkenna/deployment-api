"""Cost observability — normalized cross-cloud billing for the deployment-UI /ops/costs page.

Reads the real billing exports (GCP BigQuery `billing_export`, AWS CUR→Athena
`aws_billing.cur_uts_cost_usage`) through the UTL analytics wrappers, normalizes both
into a common `CostRecord` shape, and serves summary / breakdown / timeseries views.
GitHub is a labelled dummy provider until a billing PAT lands.

SSOT for the backends + IAM: codex/05-infrastructure/billing-cost-observability.md
"""

from deployment_api.services.cost_observability.models import (
    BreakdownResponse,
    BreakdownRow,
    CloudSummary,
    CostRecord,
    SummaryResponse,
    TimeseriesPoint,
    TimeseriesResponse,
)
from deployment_api.services.cost_observability.service import CostObservabilityService

__all__ = [
    "BreakdownResponse",
    "BreakdownRow",
    "CloudSummary",
    "CostObservabilityService",
    "CostRecord",
    "SummaryResponse",
    "TimeseriesPoint",
    "TimeseriesResponse",
]
