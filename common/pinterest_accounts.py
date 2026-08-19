"""Ad account and entity (ads / campaigns / ad groups) discovery.

All three list endpoints -- /ads, /campaigns, /ad_groups -- share the same
bookmark-pagination response shape ({"items": [...], "bookmark": ...|null}),
so list_entities() is a single generic pager used by every job; list_entity_ids()
is a thin wrapper over it for jobs that only need IDs (to batch into an
analytics call), not the full entity object.
"""

import logging

from pinterest_config import DEFAULT_PAGE_SIZE, PINTEREST_API_BASE
from pinterest_http import request_with_backoff

logger = logging.getLogger(__name__)


def list_ad_accounts(access_token: str) -> list:
    """Page through /ad_accounts to collect every ad account this token can see."""
    accounts = []
    bookmark = None
    headers = {"Authorization": f"Bearer {access_token}"}

    while True:
        params = {"page_size": DEFAULT_PAGE_SIZE}
        if bookmark:
            params["bookmark"] = bookmark

        resp = request_with_backoff(
            "GET",
            f"{PINTEREST_API_BASE}/ad_accounts",
            headers=headers,
            params=params,
        )
        body = resp.json()
        for item in body.get("items", []):
            accounts.append(item["id"])

        bookmark = body.get("bookmark")
        if not bookmark:
            break

    logger.info("Discovered %d ad account(s) via the Pinterest API", len(accounts))
    return accounts


def list_entities(ad_account_id: str, access_token: str, entity_path: str) -> list:
    """Page through /ad_accounts/{id}/{entity_path} with no filters, collecting
    the full entity object (every field the API returns) for every entity in
    the account.

    entity_path is "ads", "campaigns", or "ad_groups" -- each accepts an
    optional campaign_ids/ad_group_ids/ad_ids filter which we omit to get
    every entity in the account back in one paginated sweep.
    """
    entities = []
    bookmark = None
    headers = {"Authorization": f"Bearer {access_token}"}

    while True:
        params = {"page_size": DEFAULT_PAGE_SIZE}
        if bookmark:
            params["bookmark"] = bookmark

        resp = request_with_backoff(
            "GET",
            f"{PINTEREST_API_BASE}/ad_accounts/{ad_account_id}/{entity_path}",
            headers=headers,
            params=params,
        )
        body = resp.json()
        entities.extend(body.get("items", []))

        bookmark = body.get("bookmark")
        if not bookmark:
            break

    logger.info("Ad account %s: found %d %s", ad_account_id, len(entities), entity_path)
    return entities


def list_entity_ids(ad_account_id: str, access_token: str, entity_path: str) -> list:
    """Same as list_entities(), but returns just each entity's `id` -- for
    jobs that only need IDs to batch into an analytics call, not the full
    entity object.
    """
    return [entity["id"] for entity in list_entities(ad_account_id, access_token, entity_path)]


def resolve_ad_account_ids(args: dict, access_token: str) -> list:
    """Return the list of ad account IDs to pull.

    Normally this discovers every account the token can see via
    GET /ad_accounts. If AD_ACCOUNT_IDS was passed as a job parameter, treat
    it as an allowlist filter over the discovered accounts (rather than
    trusting it blindly) so a stale/typo'd ID doesn't silently pull zero
    accounts.
    """
    discovered = list_ad_accounts(access_token)

    requested = args.get("AD_ACCOUNT_IDS")
    if not requested:
        return discovered

    requested_ids = [a.strip() for a in requested.split(",") if a.strip()]
    discovered_set = set(discovered)
    missing = [a for a in requested_ids if a not in discovered_set]
    if missing:
        logger.warning(
            "AD_ACCOUNT_IDS included account(s) not visible to this token, skipping: %s",
            missing,
        )

    filtered = [a for a in requested_ids if a in discovered_set]
    logger.info("Restricting run to %d of %d discovered ad account(s)", len(filtered), len(discovered))
    return filtered
