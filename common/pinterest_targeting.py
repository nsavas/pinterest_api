"""Targeting-breakdown analytics (e.g. DMA/geo) and the reference lookup that
resolves targeting codes to human-readable names.

These are structurally different from pinterest_analytics.py's
fetch_analytics():
- The plain .../analytics endpoints return a flat array of {AD_ID, DATE, ...}
  rows -- one row per entity per date.
- The .../targeting_analytics endpoints return {"data": [{"targeting_type",
  "targeting_value", "metrics": {...}}]} -- one row per entity, per date,
  *per breakdown value* (e.g. per DMA). The identifier/date/metric columns
  you requested land inside "metrics", not at the top level.
- targeting_value is a bare code (e.g. "500" for a DMA), not a name. Pinterest
  resolves codes to names via a separate, non-time-series reference endpoint:
  GET /resources/targeting/{targeting_type} -- verified against Pinterest's
  published v5 OpenAPI spec on 2026-08-18. That endpoint isn't account-scoped
  (ad_account_id is an optional filter, not required), so it's called once
  per targeting_type, not once per ad account.
"""

from pinterest_config import PINTEREST_API_BASE
from pinterest_http import request_with_backoff


def fetch_targeting_analytics(ad_account_id: str, entity_path: str, id_param_name: str,
                               entity_ids: list, targeting_type: str, start_date: str,
                               end_date: str, access_token: str, columns: list) -> list:
    """Fetch daily targeting-breakdown analytics (e.g. DMA) for a batch of
    entity IDs. Returns the list under the response's "data" key -- each item
    is {"targeting_type", "targeting_value", "metrics": {...}}.

    entity_path: "ads", "campaigns", or "ad_groups"
    id_param_name: "ad_ids", "campaign_ids", or "ad_group_ids"
    targeting_type: one value from PublicTargetingType, e.g. "LOCATION" (DMA)
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "granularity": "DAY",
        "columns": ",".join(columns),
        "targeting_types": targeting_type,
        id_param_name: ",".join(entity_ids),
    }

    resp = request_with_backoff(
        "GET",
        f"{PINTEREST_API_BASE}/ad_accounts/{ad_account_id}/{entity_path}/targeting_analytics",
        headers=headers,
        params=params,
    )
    return resp.json().get("data", [])


def fetch_targeting_options(targeting_type: str, access_token: str) -> dict:
    """Fetch the code -> human-readable-name lookup for a targeting type
    (e.g. "LOCATION" for DMA codes). Global reference data, not tied to a
    specific ad account.

    The endpoint's documented response is an array containing one object
    that holds the full {code: name} mapping (see its "Sample return" in the
    OpenAPI spec) -- merge defensively in case Pinterest ever splits it
    across more than one array element.
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = request_with_backoff(
        "GET",
        f"{PINTEREST_API_BASE}/resources/targeting/{targeting_type}",
        headers=headers,
    )
    options = {}
    for item in resp.json():
        options.update(item)
    return options
