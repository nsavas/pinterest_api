"""Generic caller for Pinterest's .../analytics endpoints.

/ads/analytics, /campaigns/analytics, and /ad_groups/analytics all share the
same request shape (start_date, end_date, granularity, columns, plus one
entity-id filter param) and the same response shape: a flat JSON array of
objects, each carrying the entity's ID, a DATE field (present because we
always request granularity=DAY), and whatever columns were requested. None
of the three wrap results in a dict keyed by entity ID.

Column names come from the single `ReportingColumnSync` enum shared by all
three endpoints -- verified against Pinterest's published v5 OpenAPI spec
(https://github.com/pinterest/api-description/blob/main/v5/openapi.yaml) on
2026-08-17. Re-check that file before adding columns; the interactive docs
site is a client-rendered SPA that doesn't render for automated fetches.
"""

from pinterest_config import PINTEREST_API_BASE
from pinterest_http import request_with_backoff


def fetch_analytics(ad_account_id: str, analytics_path: str, id_param_name: str,
                     entity_ids: list, start_date: str, end_date: str,
                     access_token: str, columns: list) -> list:
    """Fetch daily analytics for a batch of entity IDs.

    analytics_path: "ads", "campaigns", or "ad_groups"
    id_param_name: "ad_ids", "campaign_ids", or "ad_group_ids"
    entity_ids: batch of IDs -- caller is responsible for respecting each
      endpoint's per-request cap (100 for ad_ids, 250 for campaign_ids /
      ad_group_ids -- see each job's ID_BATCH_SIZE constant).
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "granularity": "DAY",
        "columns": ",".join(columns),
        id_param_name: ",".join(entity_ids),
    }

    resp = request_with_backoff(
        "GET",
        f"{PINTEREST_API_BASE}/ad_accounts/{ad_account_id}/{analytics_path}/analytics",
        headers=headers,
        params=params,
    )
    return resp.json()
