# Pinterest Ads → Iceberg (AWS Glue)

Three Glue jobs that pull performance data from the Pinterest Ads API (v5) at
the ad, ad group, and campaign level, and upsert it into Iceberg tables in
S3. They share one `common/` library for everything that isn't
level-specific: OAuth token refresh, ad-account/entity discovery, HTTP
retry/backoff, incremental date-range resolution, and the Iceberg
create-table-and-merge upsert.

## Layout

```
pinterest_ads_pipeline/
├── common/                 # shared library, packaged separately for Glue
│   ├── config.py            constants (API base URL, page size, retry/backoff, lookback default)
│   ├── auth.py               Secrets Manager + Pinterest OAuth token refresh
│   ├── http.py                retry/backoff wrapper around requests
│   ├── accounts.py           ad account discovery + generic entity-ID pager (ads/campaigns/ad_groups)
│   ├── analytics.py          generic caller for the .../analytics endpoints
│   ├── dates.py               rolling-window date-range resolution + chunking
│   ├── glue_args.py           getResolvedOptions wrapper that supports optional args
│   └── iceberg.py             generic CREATE TABLE IF NOT EXISTS + MERGE INTO upsert
├── jobs/
│   ├── pinterest_ads_to_iceberg_glue_job.py
│   ├── pinterest_campaigns_to_iceberg_glue_job.py
│   └── pinterest_ad_groups_to_iceberg_glue_job.py
├── build_deps.ps1 / build_deps.sh   zip common/ for --extra-py-files
├── requirements.txt
└── README.md
```

Each file in `jobs/` only declares what's specific to its level: which
`columns` to request from the analytics endpoint, the Spark row schema, and
the Iceberg merge key. Everything else is imported from `common`.

## Deploying

`common/` isn't on the Glue job's Python path by default -- it has to be
packaged and attached via `--extra-py-files`:

```bash
./build_deps.sh          # or build_deps.ps1 on Windows
aws s3 cp pinterest_common.zip s3://<your-bucket>/pinterest_common.zip
```

For **each** of the three Glue jobs, upload the corresponding script from
`jobs/` as the job's script, and set these job parameters:

```
--datalake-formats iceberg
--additional-python-modules requests>=2.31.0
--extra-py-files s3://<your-bucket>/pinterest_common.zip
```

Then the job-specific arguments (see the docstring at the top of each script
in `jobs/` for the full list):

| Parameter | Required | Notes |
|---|---|---|
| `--SECRET_NAME` | yes | Secrets Manager secret: `{"client_id", "client_secret", "refresh_token"}` |
| `--AWS_REGION` | yes | e.g. `us-east-1` |
| `--ICEBERG_CATALOG` | yes | Glue Data Catalog name registered as an Iceberg catalog |
| `--ICEBERG_DATABASE` | yes | target database |
| `--ICEBERG_TABLE` | yes | target table (different per job, e.g. `pinterest_ad_performance`, `pinterest_campaign_performance`, `pinterest_ad_group_performance`) |
| `--ICEBERG_WAREHOUSE_PATH` | yes | `s3://bucket/prefix` |
| `--AD_ACCOUNT_IDS` | no | comma-separated allowlist; omit to auto-discover every account the token can see |
| `--START_DATE` / `--END_DATE` | no | explicit backfill range; omit for the rolling incremental window |
| `--LOOKBACK_DAYS` | no | default 14; width of the rolling window when dates are omitted |

Whenever `common/` changes, re-run `build_deps.sh`/`.ps1` and re-upload the
zip -- Glue doesn't pick up changes to an S3 object automatically on its own,
you're re-deploying the same object key.

## Design choices worth knowing about

- **Auto-discovery over a hardcoded account list.** Every job calls
  `GET /ad_accounts` and pulls every account the token can see, unless
  `AD_ACCOUNT_IDS` is passed -- in which case it's used as a validated
  allowlist filter (a stale/typo'd ID is logged and skipped, not silently
  trusted).
- **Rolling incremental window, not a high-water mark.** With no explicit
  dates, each job pulls the last `LOOKBACK_DAYS` days (default 14) ending
  yesterday. Pinterest revises spend/conversion metrics for a date within its
  attribution window after the fact, so re-pulling recent days on every run
  and relying on the Iceberg `MERGE INTO` to overwrite matching rows is what
  keeps historical data correct without a separate backfill mechanism.
- **Entity IDs are listed, then batched into analytics calls.** None of the
  three analytics endpoints will just hand you "every ad/campaign/ad group's
  data" in one call tied only to the account. Ads/analytics *can* omit
  `ad_ids`, but that risks large-response issues in practice; campaigns/
  analytics and ad_groups/analytics require the ID filter outright. So every
  job lists entity IDs first (`common.accounts.list_entity_ids`) and batches
  them into analytics calls (100 per call for ads, 250 for campaigns/ad
  groups -- Pinterest's documented per-endpoint caps).
- **Column names are pinned against the actual OpenAPI spec, not the docs
  site.** Pinterest's interactive docs
  (https://developers.pinterest.com/docs/api/v5/) are a client-rendered SPA
  that doesn't render for automated fetches. The columns in each job's
  `ANALYTICS_COLUMNS` were verified against the `ReportingColumnSync` enum in
  Pinterest's published v5 OpenAPI spec
  (https://github.com/pinterest/api-description/blob/main/v5/openapi.yaml)
  on 2026-08-17. If you add columns, check that file, not the docs site.
- **`LEADS`/`COST_PER_LEAD` over e-commerce conversion columns.** Pinterest's
  analytics enum has separate value columns per conversion type (checkout,
  signup, custom, etc.) but no generic "total conversion value." Since this
  is for an insurance company, lead-gen columns are the relevant conversion
  type here, not checkout value -- swap in the checkout/signup-specific
  columns instead if that's ever not true for a given account.
