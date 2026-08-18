"""HTTP retry/backoff wrapper shared by every Pinterest API call."""

import logging
import time

import requests

from pinterest_config import INITIAL_BACKOFF_SECONDS, MAX_RETRIES

logger = logging.getLogger(__name__)


def request_with_backoff(method: str, url: str, headers: dict,
                          max_retries: int = MAX_RETRIES,
                          initial_backoff: int = INITIAL_BACKOFF_SECONDS,
                          **kwargs) -> requests.Response:
    """requests.request() with retry on 429 (honoring Retry-After) and 5xx.

    Raises via resp.raise_for_status() for any other error status, and after
    exhausting max_retries.
    """
    backoff = initial_backoff
    resp = None
    for attempt in range(1, max_retries + 1):
        resp = requests.request(method, url, headers=headers, timeout=60, **kwargs)
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", backoff))
            logger.warning("Rate limited by Pinterest API, sleeping %ss (attempt %s/%s)",
                            retry_after, attempt, max_retries)
            time.sleep(retry_after)
            backoff *= 2
            continue
        if resp.status_code >= 500:
            logger.warning("Pinterest API %s error, retrying in %ss (attempt %s/%s)",
                            resp.status_code, backoff, attempt, max_retries)
            time.sleep(backoff)
            backoff *= 2
            continue
        resp.raise_for_status()
        return resp
    resp.raise_for_status()  # last attempt's error, if we fell through
    return resp
