# Pinterest Ads Pipeline — API Scope

Scope of the Pinterest Ads API surface consumed by the `pinterest_ads_pipeline` Glue jobs: how the jobs authenticate, the object model they traverse, the endpoints they call, the breakdowns they request, and the metrics they pull.

All endpoints are on **Pinterest API v5** (`https://api.pinterest.com/v5`). Reference: [Pinterest Developer Platform — API v5 reference](https://developers.pinterest.com/docs/api/v5/)

---

## Authentication

The pipeline authenticates as a Pinterest app using the **OAuth 2.0 refresh token grant**. Pinterest access tokens are short-lived, so no access token is cached between runs — every job run mints a fresh one before making any data calls.

### Credential storage

Credentials live in a single AWS Secrets Manager secret, passed to each job as `--SECRET_NAME`, containing:

```json
{
  "client_id": "...",
  "client_secret": "...",
  "refresh_token": "..."
}
```

### Token exchange on every run

| Step | Detail |
|---|---|
| 1. Read secret | `boto3` Secrets Manager `get_secret_value`, JSON-decoded |
| 2. Build Basic auth | `base64(client_id:client_secret)` → `Authorization: Basic <encoded>` |
| 3. Exchange | `POST /v5/oauth/token` with `grant_type=refresh_token` and the stored refresh token, form-encoded |
| 4. Use | The returned `access_token` is sent on every subsequent call as `Authorization: Bearer <access_token>` |

### Refresh token rotation

Pinterest may return a **new** `refresh_token` in the exchange response. When the returned value differs from the stored one, the job writes the new value back to Secrets Manager (`put_secret_value`) before continuing. Without this, a rotation would silently break the next scheduled run.

> **Operational note:** the refresh token itself expires (~60 days on Pinterest's standard terms). Because every run performs an exchange, an actively scheduled pipeline keeps itself alive; a pipeline paused longer than the refresh token's lifetime needs the secret re-seeded by hand through the OAuth consent flow.

Reference: [Pinterest — Authentication](https://developers.pinterest.com/docs/getting-started/authentication/)

---

## Object Hierarchy

Pinterest's advertising objects nest four levels deep. Every performance endpoint is scoped to an ad account and addresses entities at one of these levels.

```
Ad Account            (account-level container; billing + asset ownership)
   └── Campaign       (objective + budget/flight)
        └── Ad Group  (targeting, bid, schedule)
             └── Ad   (creative + destination)
```

| Level | ID field | Notes |
|---|---|---|
| Ad Account | `ad_account_id` | Discovered via `GET /v5/ad_accounts`; the root path segment of nearly every other call |
| Campaign | `campaign_id` | Carries the objective type (`CAMPAIGN_OBJECTIVE_TYPE`) |
| Ad Group | `ad_group_id` | Pinterest's equivalent of **Meta's "ad set"** — the layer holding targeting, bid, and schedule |
| Ad | `ad_id` | The creative unit; all demographic and geographic breakdowns in this pipeline are taken at this level |

---

## Key Endpoints

| Purpose | Method and path | Used by |
|---|---|---|
| Token exchange | `POST /v5/oauth/token` | Every job (auth bootstrap) |
| Ad account discovery | `GET /v5/ad_accounts` | Every job |
| Entity listing | `GET /v5/ad_accounts/{ad_account_id}/campaigns` (also `/ad_groups`, `/ads`) | Dimensions job (full objects); performance jobs (IDs only) |
| Performance analytics | `GET /v5/ad_accounts/{ad_account_id}/ads/analytics` (also `/campaigns`, `/ad_groups`) | Ad, campaign, ad group performance jobs |
| Breakdown analytics | `GET /v5/ad_accounts/{ad_account_id}/ads/targeting_analytics` | DMA, gender, and age breakdown jobs |
| Targeting reference data | `GET /v5/resources/targeting/{targeting_type}` | DMA reference job |

### Analytics request parameters

Both analytics endpoints take the same core parameters:

| Parameter | Value used | Notes |
|---|---|---|
| `start_date` / `end_date` | `YYYY-MM-DD` | Inclusive; resolved from the lookback window or explicit job arguments |
| `granularity` | `DAY` | Produces one row per entity per day rather than one aggregate for the range |
| `columns` | Comma-separated metric enum values | See **Performance Metrics** below |
| `ad_ids` / `campaign_ids` / `ad_group_ids` | Comma-separated batch of entity IDs | Required — analytics is requested *for a specific set of entities*, not for a whole account |
| `targeting_types` | `LOCATION`, `GENDER`, or `AGE_BUCKET` | `targeting_analytics` only |

### Entity-ID batching limits

Unlike Meta, Pinterest's analytics endpoints do **not** accept a "whole account" request — the caller must supply explicit entity IDs, subject to per-request caps. Each job lists entity IDs first, then chunks them:

| Endpoint | ID parameter | Max IDs per request |
|---|---|---|
| `/ads/analytics` | `ad_ids` | **100** |
| `/campaigns/analytics` | `campaign_ids` | **250** |
| `/ad_groups/analytics` | `ad_group_ids` | **250** |
| `/ads/targeting_analytics` | `ad_ids` | **250** |

This "list IDs, then batch" pattern is the main structural difference from the Meta pipeline, where a single request covers every entity at a level.

---

## Report Breakdowns

Breakdowns come from the `targeting_analytics` endpoint via the `targeting_types` parameter. Each breakdown returns rows shaped as `{targeting_type, targeting_value, metrics: {...}}` — the breakdown value arrives in a sibling field rather than merged into the metrics object.

All breakdowns in this pipeline are taken at the **ad level** only.

| Glue job | Endpoint | `targeting_types` | Breakdown column | Row grain |
|---|---|---|---|---|
| `pinterest_ads_dma_to_iceberg_glue_job.py` | `/ads/targeting_analytics` | `LOCATION` | `dma_code` | ad × date × DMA |
| `pinterest_ads_gender_to_iceberg_glue_job.py` | `/ads/targeting_analytics` | `GENDER` | `gender` | ad × date × gender |
| `pinterest_ads_age_to_iceberg_glue_job.py` | `/ads/targeting_analytics` | `AGE_BUCKET` | `age_bucket` | ad × date × age bucket |
| `pinterest_dma_reference_to_iceberg_glue_job.py` | `/resources/targeting/LOCATION` | n/a (reference lookup) | `dma_code` → `dma_name` | one row per DMA |
| `pinterest_ads_to_iceberg_glue_job.py` | `/ads/analytics` | none | — | ad × date |
| `pinterest_campaigns_to_iceberg_glue_job.py` | `/campaigns/analytics` | none | — | campaign × date |
| `pinterest_ad_groups_to_iceberg_glue_job.py` | `/ad_groups/analytics` | none | — | ad group × date |
| `pinterest_dimensions_to_iceberg_glue_job.py` | entity listing endpoints | none | — | current-state snapshot |

> **Important — gender and age are separate breakdowns, not a cross-tab.** Pinterest reports `GENDER` and `AGE_BUCKET` as two independent breakdowns of the same totals. They are written to two separate tables deliberately: summing across both would double-count every metric. (Meta, by contrast, permits a true `age` + `gender` cross-tab in one request.)

> **`LOCATION` returns bare DMA codes**, not names — hence the separate reference job that populates the code-to-name lookup table.

---

## Performance Metrics

Metrics are requested by enum name via the `columns` parameter. The set below is held **identical across the ad, campaign, ad group, DMA, gender, and age jobs**, so a metric summed up from the ad level is directly comparable to the same metric reported at the campaign level.

Values are validated against Pinterest's published `ReportingColumnSync` enum in the [Pinterest OpenAPI specification](https://github.com/pinterest/api-description).

### Identifiers and dimensional context

| Column | Present on | Description |
|---|---|---|
| `AD_ACCOUNT_ID` | all | Owning ad account |
| `AD_ID` | ad-level jobs | Ad identifier |
| `AD_GROUP_ID` | ad, ad group jobs | Parent ad group |
| `CAMPAIGN_ID` | all | Parent campaign |
| `CAMPAIGN_NAME` | campaign job | Campaign name |
| `CAMPAIGN_ENTITY_STATUS` | campaign job | Campaign status at report time |
| `CAMPAIGN_OBJECTIVE_TYPE` | campaign job | Campaign objective |
| `AD_GROUP_NAME` | ad group job | Ad group name |
| `AD_GROUP_ENTITY_STATUS` | ad group job | Ad group status at report time |

### Spend and cost efficiency

| Column | Description |
|---|---|
| `SPEND_IN_DOLLAR` | Amount spent, in dollars |
| `ECPC_IN_DOLLAR` | Effective cost per click |
| `CPM_IN_DOLLAR` | Cost per thousand impressions |
| `COST_PER_LEAD` | Cost per lead conversion |

### Delivery

| Column | Description |
|---|---|
| `TOTAL_IMPRESSION` | Impressions served |

### Engagement

| Column | Description |
|---|---|
| `TOTAL_CLICKTHROUGH` | Clickthroughs |
| `TOTAL_ENGAGEMENT` | Total engagements (clicks, saves, closeups, and similar) |
| `CTR` | Clickthrough rate |

### Conversion

| Column | Description |
|---|---|
| `TOTAL_CONVERSIONS` | All attributed conversions |
| `LEADS` | Lead conversions specifically — the primary conversion type for this org's insurance vertical |

### Video funnel

| Column | Description |
|---|---|
| `TOTAL_VIDEO_P0_COMBINED` | Video starts (0% quartile) |
| `TOTAL_VIDEO_P25_COMBINED` | Reached 25% |
| `TOTAL_VIDEO_P50_COMBINED` | Reached 50% |
| `TOTAL_VIDEO_P75_COMBINED` | Reached 75% |
| `TOTAL_VIDEO_P100_COMPLETE` | Completed views |

> Unlike Meta — where conversion and video metrics arrive as nested `list<AdsActionStats>` objects requiring JSON storage — every Pinterest metric above is a **flat named scalar**, so each maps to its own typed Iceberg column.
