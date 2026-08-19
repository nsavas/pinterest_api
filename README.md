# Pinterest Ads → Iceberg (AWS Glue)

Eight Glue jobs that pull data from the Pinterest Ads API (v5) and write it
into Iceberg tables in S3:

- **Three performance jobs** at the ad, ad group, and campaign level.
- **One DMA breakdown job**, at the ad level only, giving ad-level performance
  broken down by DMA (Designated Market Area) -- a geographic dimension, not
  another metric on the existing ad table (see "Why DMA is a separate job"
  below for why it can't just be another column).
- **One DMA reference job** that resolves the bare DMA codes in that table to
  human-readable names.
- **Two demographic breakdown jobs**, at the ad level only, giving ad-level
  performance broken down by gender and, separately, by age bucket -- two
  jobs writing two tables, not one combined table (see "Demographic jobs"
  below for why that split matters).
- **One dimensions job** that pulls the full Campaign/Ad Group/Ad metadata
  objects (name, status, budget, targeting, creative settings -- every field,
  no metrics) into three tables in a single run (see "Dimensions job" below).

All eight share one `common/` library for everything that isn't
job-specific: OAuth token refresh, ad-account/entity discovery, HTTP
retry/backoff, incremental date-range resolution, and the Iceberg
write helpers (upsert for time-series fact data, full-replace for reference/
dimension data).

## Layout

```
pinterest_ads_pipeline/
├── common/                       # shared modules, zipped flat for --extra-py-files (see "Deploying")
│   ├── pinterest_config.py        constants (API base URL, page size, retry/backoff, lookback default)
│   ├── pinterest_auth.py          Secrets Manager + Pinterest OAuth token refresh
│   ├── pinterest_http.py          retry/backoff wrapper around requests
│   ├── pinterest_accounts.py      ad account discovery + generic entity pager (full objects or just IDs)
│   ├── pinterest_analytics.py     generic caller for the .../analytics endpoints
│   ├── pinterest_targeting.py     generic caller for .../targeting_analytics (e.g. DMA breakdown) + the code->name lookup
│   ├── pinterest_dates.py         rolling-window date-range resolution + chunking
│   ├── pinterest_glue_args.py     getResolvedOptions wrapper that supports optional args
│   └── pinterest_iceberg.py       upsert() for time-series fact tables, replace_table() for reference tables
├── jobs/
│   ├── pinterest_ads_to_iceberg_glue_job.py
│   ├── pinterest_campaigns_to_iceberg_glue_job.py
│   ├── pinterest_ad_groups_to_iceberg_glue_job.py
│   ├── pinterest_ads_dma_to_iceberg_glue_job.py             ad-level performance broken down by DMA
│   ├── pinterest_dma_reference_to_iceberg_glue_job.py        DMA code -> name lookup table
│   ├── pinterest_ads_gender_to_iceberg_glue_job.py           ad-level performance broken down by gender
│   ├── pinterest_ads_age_to_iceberg_glue_job.py              ad-level performance broken down by age bucket
│   └── pinterest_dimensions_to_iceberg_glue_job.py           campaign/ad group/ad metadata (no metrics)
├── build_deps.ps1 / build_deps.sh   zip common/'s contents (flat) for --extra-py-files
├── requirements.txt
└── README.md
```

Each file in `jobs/` only declares what's specific to it: which `columns`
to request, the Spark row schema, and the Iceberg merge key (or, for the
reference job, nothing but the fetch + full-replace write). Everything else
is imported from the `pinterest_*` modules in `common/`.

**Why these are flat modules (`pinterest_auth.py`), not a `common` package
(`common/auth.py` + `__init__.py`) imported as `common.auth`:** that's the
more obvious-looking structure, and it's exactly what this project used at
first -- AWS's own docs describe it as the correct way to zip a Python
package for `--extra-py-files`. It still failed with `ModuleNotFoundError:
No module named 'common'` on an actual Glue job run, matching a
[known unresolved AWS Glue issue](https://github.com/awslabs/aws-glue-libs/issues/173)
with zipimport + `--extra-py-files` and nested packages. Flat modules are
the other officially-documented pattern
([PySpark native features](https://spark.apache.org/docs/latest/api/python/tutorial/python_packaging.html#using-pyspark-native-features)) --
each `.py` file becomes directly importable once its containing zip is added
to `sys.path`, with no `__init__.py`/package resolution involved to go
wrong. Every module is prefixed `pinterest_` specifically because a flat
global namespace can collide with unrelated modules already on the Glue
worker's `sys.path` -- `pinterest_http.py` avoids shadowing the Python
stdlib's own `http` package, which `requests`, `boto3`, and Spark's own
internals all rely on.

## Deploying

`common/` isn't on the Glue job's Python path by default -- it has to be
packaged and attached via `--extra-py-files`. The build scripts zip the
*contents* of `common/` at the zip's root (no wrapping folder) -- see the
"Why these are flat modules" note above for why that matters:

```bash
./build_deps.sh          # or build_deps.ps1 on Windows
aws s3 cp pinterest_common.zip s3://<your-bucket>/pinterest_common.zip
```

For **each** of the eight Glue jobs, upload the corresponding script from
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
| `--ICEBERG_TABLE` | yes\*\* | target table (different per job -- see below) |
| `--ICEBERG_WAREHOUSE_PATH` | yes | `s3://bucket/prefix` |
| `--AD_ACCOUNT_IDS` | no* | comma-separated allowlist; omit to auto-discover every account the token can see |
| `--START_DATE` / `--END_DATE` | no* | explicit backfill range; omit for the rolling incremental window |
| `--LOOKBACK_DAYS` | no* | default 14; width of the rolling window when dates are omitted |

\* The six performance/DMA/demographic jobs take all three optional args.
`pinterest_dma_reference_to_iceberg_glue_job.py` and
`pinterest_dimensions_to_iceberg_glue_job.py` take **none** of them --
neither is time-series data, so each only needs the required arguments.

\*\* `pinterest_dimensions_to_iceberg_glue_job.py` writes three tables in one
run, so instead of a single `--ICEBERG_TABLE` it takes three:
`--ICEBERG_TABLE_CAMPAIGNS`, `--ICEBERG_TABLE_AD_GROUPS`, `--ICEBERG_TABLE_ADS`.

Suggested table names, one per job:

| Job | Table(s) |
|---|---|
| `pinterest_ads_to_iceberg_glue_job.py` | `pinterest_ad_performance` |
| `pinterest_campaigns_to_iceberg_glue_job.py` | `pinterest_campaign_performance` |
| `pinterest_ad_groups_to_iceberg_glue_job.py` | `pinterest_ad_group_performance` |
| `pinterest_ads_dma_to_iceberg_glue_job.py` | `pinterest_ad_dma_performance` |
| `pinterest_dma_reference_to_iceberg_glue_job.py` | `pinterest_dma_reference` |
| `pinterest_ads_gender_to_iceberg_glue_job.py` | `pinterest_ad_gender_performance` |
| `pinterest_ads_age_to_iceberg_glue_job.py` | `pinterest_ad_age_performance` |
| `pinterest_dimensions_to_iceberg_glue_job.py` | `pinterest_campaign_dim`, `pinterest_ad_group_dim`, `pinterest_ad_dim` |

Whenever `common/` changes, re-run `build_deps.sh`/`.ps1` and re-upload the
zip -- Glue doesn't pick up changes to an S3 object automatically on its own,
you're re-deploying the same object key. Adding `pinterest_targeting.py`, or
`list_entities()` in `pinterest_accounts.py`, both count as `common/`
changes, so rebuild and re-upload if you're picking up the DMA or dimensions
jobs after already having deployed the earlier ones. The two demographic
jobs don't need this -- they reuse `fetch_targeting_analytics()` unchanged
(see "Demographic jobs" below for why).

## Demographic jobs

`pinterest_ads_gender_to_iceberg_glue_job.py` and
`pinterest_ads_age_to_iceberg_glue_job.py` give ad-level performance broken
down by gender and by age bucket, respectively. Same endpoint family as the
DMA job (`ads/targeting_analytics`), requesting `GENDER` or `AGE_BUCKET`
instead of `LOCATION` -- one `targeting_type`, one job, one table each.

- **Two separate jobs/tables, not one combined table.** An earlier version
  of this requested both breakdowns in a single job, writing one table with
  a `demographic_type` column distinguishing `GENDER` rows from `AGE_BUCKET`
  rows. That worked, but every query against it had to remember to filter
  to one `demographic_type` before aggregating -- `GENDER` and `AGE_BUCKET`
  are two *independent* breakdowns of the same underlying traffic (Pinterest
  says so explicitly), so summing across both silently doubles every metric.
  Splitting into two single-purpose tables removes that footgun entirely:
  every row in `pinterest_ad_gender_performance` is already scoped to one
  gender, so `SUM(spend)` for an `(ad_id, stat_date)` just works with no
  `WHERE` clause needed. The same double-counting still applies if you ever
  join or union the two tables together and sum across both -- that's
  inherent to the data (two independent slices of the same traffic), not
  something table structure can fully hide.
- **`GENDER`/`AGE_BUCKET` values are already human-readable** (`"female"`,
  `"45-49"`) -- unlike DMA codes, no separate reference/lookup table is
  needed for either.
- **Deliberately excludes `AGE_BUCKET_AND_GENDER`.** That's the true age x
  gender cross-tab, but Pinterest's spec flags it as "BETA and not yet
  available to all users." Worth adding as a third job once confirmed
  enabled on your accounts, rather than building against an unconfirmed
  BETA field now.
- Both jobs pass their single `targeting_type` straight through
  `fetch_targeting_analytics()` unchanged -- no `common/` changes were
  needed to add either.

## Dimensions job

`pinterest_dimensions_to_iceberg_glue_job.py` is structurally different from
the other seven, since it's pulling metadata, not performance data:

- **Same account-discovery pattern, no batching needed.** Like every other
  job, it lists every ad account first, then pulls per account -- but unlike
  the analytics endpoints, `GET /ad_accounts/{id}/campaigns` (and
  `/ad_groups`, `/ads`) return the *full* entity object directly with no
  `columns` selector and no ID-list filter required, so there's no separate
  "list IDs then fetch details in batches" step.
- **All available fields, not just a curated subset.** Every scalar field
  (string/int/bool/number) each entity's OpenAPI schema defines becomes its
  own typed column. Every nested object or array field (`targeting_spec`,
  `tracking_urls`, `rejected_reasons`, `carting_products`, etc.) is
  serialized to a `..._json` string column instead of a native Spark
  struct/array column -- deliberately, since Pinterest's nested ad-config
  shapes vary by campaign objective and creative type, and a rigid Spark
  schema would break or silently null fields the moment Pinterest returns a
  shape it wasn't built from. Query those columns with `from_json`/
  `get_json_object` in Spark or `json_extract` in Athena.
- **Full replace per table, not merge/upsert**, same reasoning as the DMA
  reference table: a merge only ever adds or updates rows, so a
  campaign/ad group/ad deleted on Pinterest's side would never disappear
  from the table. All three tables are written once, at the very end, after
  every account has been fully fetched, so a mid-run failure leaves the
  existing tables untouched rather than partially overwritten.
- Both entity schemas' exact fields and types were verified against
  Pinterest's OpenAPI spec on 2026-08-18, including two that are easy to get
  wrong by guessing: `customer_segment_id` is a numeric *string*
  (`Pinterest.Lib.IntegerFormatType`), not an integer, and `dca_assets` has
  no declared type in the spec at all -- exactly why it's JSON-serialized
  rather than assumed to be a well-typed object.

## Why DMA is a separate job, not a column

It's tempting to expect `dma_code` could just be another value in
`ANALYTICS_COLUMNS` on the existing ad-level job. It can't, for two reasons
confirmed against Pinterest's OpenAPI spec:

1. **Different endpoint, different response shape.** `ads/analytics` (used by
   `pinterest_ads_to_iceberg_glue_job.py`) has no geo-breakdown parameter at
   all. DMA-level data only comes from `ads/targeting_analytics`, called with
   `targeting_types=LOCATION`. Its response nests each row's requested
   columns inside a `"metrics"` object, as siblings of `"targeting_type"` /
   `"targeting_value"` -- not the flat `{AD_ID, DATE, ...}` shape
   `ads/analytics` returns. And critically, it returns **one row per (ad,
   date, DMA)**, not one row per (ad, date) -- adding it to the existing
   table would silently multiply every row by however many DMAs that ad had
   spend in.
2. **The DMA code has no name attached.** `targeting_value` for a `LOCATION`
   row is a bare code (e.g. `"500"`), not a name. Resolving it requires a
   second, unrelated endpoint: `GET /resources/targeting/LOCATION`, which
   isn't ad-account-scoped and isn't time-series data -- it's Pinterest's
   current reference mapping of every code to a name. That's why it's its
   own job with its own write pattern (full replace, not merge -- see
   `pinterest_iceberg.py`'s `replace_table()`), not a lookup embedded in the
   fact job.

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
  job lists entity IDs first (`pinterest_accounts.list_entity_ids`) and
  batches them into analytics calls (100 per call for ads, 250 for
  campaigns/ad groups -- Pinterest's documented per-endpoint caps).
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
- **`LOCATION` is the DMA breakdown, not `REGION` or `GEO`.** All three are
  valid `targeting_types` values and all three are geographic, but at
  different granularity -- confirmed from the OpenAPI spec's example
  targeting_values: `REGION` returns state-level codes (`"US-CA"`), `GEO`
  returns ZIP-level (`"US:94102"`), and `LOCATION` returns DMA-level
  (`"500"`, Nielsen-style numeric codes). `LOCATION` is the one that's DMA.
- **The DMA fact table reuses the same `ANALYTICS_COLUMNS` metric list as
  the plain ad-level job**, on purpose -- so summing `pinterest_ad_dma_performance`
  across every `dma_code` for a given `(ad_id, stat_date)` should
  approximately reproduce that row in `pinterest_ad_performance` (modulo any
  traffic Pinterest doesn't attribute to a DMA), which is a useful sanity
  check when validating the DMA data.
