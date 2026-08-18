"""Shared constants for the Pinterest Ads API clients."""

PINTEREST_API_BASE = "https://api.pinterest.com/v5"

# Page size used for every paginated /list endpoint (ad_accounts, ads,
# campaigns, ad_groups). 100 is comfortably under Pinterest's max and keeps
# each page's response small.
DEFAULT_PAGE_SIZE = 100

# Default width of the rolling incremental pull when a job isn't given
# explicit START_DATE/END_DATE. See common/dates.py for why 14 days.
DEFAULT_LOOKBACK_DAYS = 14

# HTTP retry/backoff defaults for common/http.py's request_with_backoff().
MAX_RETRIES = 5
INITIAL_BACKOFF_SECONDS = 2
