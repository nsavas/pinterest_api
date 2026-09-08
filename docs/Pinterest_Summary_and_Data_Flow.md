# Pinterest Ads Pipeline — Summary & Data Flow

## Overview

`pinterest_ads_pipeline` is a set of **eight AWS Glue (PySpark) jobs** that pull advertising data from the Pinterest Ads API v5 and land it in **Apache Iceberg** tables in S3, registered in the AWS Glue Data Catalog.

The pipeline covers four kinds of data:

- **Performance facts** — daily spend, delivery, engagement, conversion, and video metrics at the ad, ad group, and campaign levels.
- **Breakdown facts** — the same ad-level metrics split by geography (DMA), gender, and age bucket.
- **Dimensions** — full current-state metadata for every campaign, ad group, and ad (name, status, budget, targeting, creative).
- **Reference data** — the DMA code-to-name lookup needed to make the geographic breakdown table readable.

Every job discovers its own ad accounts (`GET /v5/ad_accounts`) rather than taking a hardcoded list, so newly granted accounts flow in automatically. An optional `AD_ACCOUNT_IDS` argument narrows a run to a subset, applied as an allowlist filter over discovered accounts so a stale or mistyped ID cannot silently produce zero rows.

### Lookback logic

The six performance and breakdown jobs are designed to run on a **daily schedule with no date arguments**. In that mode each run pulls a rolling window:

- **Window width** — `LOOKBACK_DAYS`, default **14 days**.
- **Anchor** — the window *ends yesterday*, never today. Today's metrics are still accumulating on Pinterest's side, so pulling them would land a partial day.
- **Resulting window** — `[yesterday − (LOOKBACK_DAYS − 1), yesterday]`, inclusive.

Re-pulling a full 14-day window on every run — rather than just yesterday — is deliberate. Pinterest revises spend and conversion metrics for a date after the fact as attribution windows close, so a date's numbers are not final the day after it happens. Because every performance table is written with an Iceberg `MERGE INTO` keyed on entity + date, re-pulling overlapping days **overwrites** existing rows instead of duplicating them. Late-arriving attribution corrects itself without a separate backfill mechanism.

For one-off backfills, passing both `START_DATE` and `END_DATE` overrides the rolling window entirely and the job pulls exactly that range.

The dimensions and DMA reference jobs take no date arguments at all — they are current-state snapshots, not time series.

---

## Iceberg Tables

| Table name | Job | Job type | Description |
|---|---|---|---|
| `pinterest_ad_performance` | `pinterest_ads_to_iceberg_glue_job.py` | Fact | Daily ad-level performance. One row per ad per day. |
| `pinterest_ad_group_performance` | `pinterest_ad_groups_to_iceberg_glue_job.py` | Fact | Daily ad-group-level performance. One row per ad group per day. |
| `pinterest_campaign_performance` | `pinterest_campaigns_to_iceberg_glue_job.py` | Fact | Daily campaign-level performance. One row per campaign per day. |
| `pinterest_ad_dma_performance` | `pinterest_ads_dma_to_iceberg_glue_job.py` | Fact (breakdown) | Ad-level performance split by Nielsen DMA. One row per ad per day per DMA. Join `dma_code` to `pinterest_dma_reference` for names. |
| `pinterest_ad_gender_performance` | `pinterest_ads_gender_to_iceberg_glue_job.py` | Fact (breakdown) | Ad-level performance split by gender. One row per ad per day per gender. |
| `pinterest_ad_age_performance` | `pinterest_ads_age_to_iceberg_glue_job.py` | Fact (breakdown) | Ad-level performance split by age bucket. One row per ad per day per bucket. |
| `pinterest_campaign_dim` | `pinterest_dimensions_to_iceberg_glue_job.py` | Dimension | Current-state campaign metadata. Fully replaced each run. |
| `pinterest_ad_group_dim` | `pinterest_dimensions_to_iceberg_glue_job.py` | Dimension | Current-state ad group metadata, including targeting specs. Fully replaced each run. |
| `pinterest_ad_dim` | `pinterest_dimensions_to_iceberg_glue_job.py` | Dimension | Current-state ad metadata, including creative details. Fully replaced each run. |
| `pinterest_dma_reference` | `pinterest_dma_reference_to_iceberg_glue_job.py` | Reference | DMA code to human-readable market name lookup. Fully replaced each run. |

> The three dimension tables are written by a **single job** in one run, so it takes three table arguments (`ICEBERG_TABLE_CAMPAIGNS`, `ICEBERG_TABLE_AD_GROUPS`, `ICEBERG_TABLE_ADS`) instead of one.

---

## Design Notes

**Partitioning.** Every fact table is partitioned by `days(stat_date)` — an Iceberg hidden partition transform on the date column. This matches the dominant query pattern (date-bounded reporting) and aligns with the write pattern: a run touching a 14-day window rewrites only those day-partitions. Dimension and reference tables are unpartitioned; they are small, fully replaced each run, and always queried in full.

**Synchronous execution.** All Pinterest calls are synchronous request/response. Pinterest's analytics endpoints return results inline, so there is no report-job submission or polling step. (The sibling Meta pipeline is asynchronous — Meta both offers and recommends async reporting at volume. Pinterest offers no equivalent for this data.)

**Upsert vs. full replace.** Two write patterns, chosen per table type:

- **Fact tables use `MERGE INTO`** (`CREATE TABLE IF NOT EXISTS` + merge on the key columns). Re-running an overlapping date range updates rows in place, which is what makes the rolling lookback window safe.
- **Dimension and reference tables use `CREATE OR REPLACE TABLE`**. A merge would only ever add or update rows, so an entity deleted on Pinterest's side would linger forever. A full replace makes the table match Pinterest's current state exactly on every run.

**Merge keys.** Each fact table's key is the entity plus the date, plus the breakdown value where one exists:

| Table | Merge key |
|---|---|
| `pinterest_ad_performance` | `ad_account_id`, `ad_id`, `stat_date` |
| `pinterest_ad_group_performance` | `ad_account_id`, `ad_group_id`, `stat_date` |
| `pinterest_campaign_performance` | `ad_account_id`, `campaign_id`, `stat_date` |
| `pinterest_ad_dma_performance` | `ad_account_id`, `ad_id`, `dma_code`, `stat_date` |
| `pinterest_ad_gender_performance` | `ad_account_id`, `ad_id`, `gender`, `stat_date` |
| `pinterest_ad_age_performance` | `ad_account_id`, `ad_id`, `age_bucket`, `stat_date` |

**Entity-ID batching.** Pinterest's analytics endpoints require explicit entity IDs and cap how many fit per request (100 for `ad_ids` on `/ads/analytics`; 250 elsewhere). Each performance job therefore lists entity IDs for an account first, then chunks them into compliant batches. This is the pipeline's main source of API call volume.

**Date handling.** `stat_date` is carried through the Spark DataFrame as a `YYYY-MM-DD` **string** and cast to a real `date` inside the Iceberg merge (`CAST(stat_date AS date)`). This avoids Spark/driver timezone ambiguity on the way in while still producing a properly typed, partitionable date column in the table.

**Flat shared modules.** The shared `common/` library is packaged as a zip with the `.py` files **at the root** — no wrapping package directory, no `__init__.py` — and attached via `--extra-py-files`. The more conventional package layout (which AWS's own documentation describes) fails at runtime with `ModuleNotFoundError` on real Glue jobs, matching a [known unresolved AWS Glue issue](https://github.com/awslabs/aws-glue-libs/issues/173) with `zipimport` and nested packages. Each module carries a `pinterest_` prefix to keep the flat namespace collision-free.

**Retry and backoff.** All calls run through a shared wrapper that retries on HTTP 429 (honoring `Retry-After`) and 5xx, with exponential backoff, up to 5 attempts.

---

## DMA vs. Comscore Market

**This pipeline uses Nielsen DMA and is not affected by Meta's migration** — but the contrast matters when comparing Pinterest and Meta geographic data side by side, so it is documented here.

### What this pipeline does

Pinterest exposes its geographic breakdown through the `LOCATION` targeting type on `/ads/targeting_analytics`, which maps to the classic **Nielsen Designated Market Area** model: 210 US media markets, each a group of counties sharing a dominant broadcast television signal. Pinterest returns bare numeric DMA codes, so `pinterest_dma_reference_to_iceberg_glue_job.py` pulls the code-to-name lookup from `/v5/resources/targeting/LOCATION` into a reference table. Join it to `pinterest_ad_dma_performance.dma_code` for readable market names.

### What changed on Meta — and why it does not apply here

Meta **retired Nielsen DMA on 22 June 2026**, replacing it with a new **Comscore Markets** breakdown. Comscore introduced its market definitions in August 2025 and, like Nielsen, covers **210 US markets** — but builds them from both linear television and digital signals, where Nielsen's framework grew out of television alone. Comscore holds MRC accreditation for national and local television measurement (March 2024) and for demographic television metrics across all 210 local markets (April 2025), and reports measuring roughly one in every two to five households per local market, against Nielsen's one in roughly 1,600 to 2,000.

**Pinterest has announced no equivalent change.** Its `LOCATION` breakdown continues to return Nielsen DMA codes.

### Practical consequence for cross-platform analysis

Pinterest DMA data and Meta Comscore Market data are **not directly joinable**. Both describe 210 US markets and their names often coincide, but the underlying county-to-market assignments come from different methodologies and different vendors. Treat them as separate geographic taxonomies. If a unified cross-platform market view is needed, build an explicit crosswalk and validate it rather than joining on market name.

**References**

- [Comscore vs. Nielsen: media measurement compared](https://m-marketingconsultants.com/comscore-vs-nielsen-popular-media-measurements-explained/)
- [What Meta's DMA switch means for geo testing — Measured](https://www.measured.com/blog/what-metas-dma-switch-means-for-geo-testing/)
- [Meta switches to Comscore Markets data — Social Media Today](https://www.socialmediatoday.com/news/meta-switches-to-comscore-markets-data/814886/)
- [Pinterest targeting reference (`LOCATION`)](https://developers.pinterest.com/docs/api/v5/)

---

## Data Dictionary

### Fact tables — shared columns

Present on all six performance and breakdown tables unless noted.

| Column | Data type | Category | Description |
|---|---|---|---|
| `ad_account_id` | string | Identifier | Owning Pinterest ad account. Populated from the account being queried, not from the API payload. |
| `ad_id` | string | Identifier | Ad identifier. Ad-level tables only. |
| `ad_group_id` | string | Identifier | Parent ad group. Ad and ad group tables. |
| `campaign_id` | string | Identifier | Parent campaign. |
| `campaign_name` | string | Dimension | Campaign name. Campaign table only. |
| `campaign_status` | string | Dimension | Campaign status at report time. Campaign table only. |
| `campaign_objective_type` | string | Dimension | Campaign objective type. Campaign table only. |
| `ad_group_name` | string | Dimension | Ad group name. Ad group table only. |
| `ad_group_status` | string | Dimension | Ad group status at report time. Ad group table only. |
| `stat_date` | date | Time | The metrics date. Partition column. |
| `spend` | double | Spend | Amount spent in dollars (`SPEND_IN_DOLLAR`). |
| `impressions` | bigint | Delivery | Impressions served (`TOTAL_IMPRESSION`). |
| `clicks` | bigint | Engagement | Clickthroughs (`TOTAL_CLICKTHROUGH`). |
| `engagements` | bigint | Engagement | Total engagements — clicks, saves, closeups (`TOTAL_ENGAGEMENT`). |
| `ctr` | double | Engagement | Clickthrough rate (`CTR`). |
| `conversions` | bigint | Conversion | All attributed conversions (`TOTAL_CONVERSIONS`). |
| `leads` | bigint | Conversion | Lead conversions (`LEADS`) — the primary conversion type for this org. |
| `cost_per_lead` | double | Cost efficiency | Cost per lead (`COST_PER_LEAD`). |
| `ecpc` | double | Cost efficiency | Effective cost per click (`ECPC_IN_DOLLAR`). |
| `cpm` | double | Cost efficiency | Cost per thousand impressions (`CPM_IN_DOLLAR`). |
| `video_p0_combined` | bigint | Video | Video starts (`TOTAL_VIDEO_P0_COMBINED`). |
| `video_p25_combined` | bigint | Video | Reached 25% (`TOTAL_VIDEO_P25_COMBINED`). |
| `video_p50_combined` | bigint | Video | Reached 50% (`TOTAL_VIDEO_P50_COMBINED`). |
| `video_p75_combined` | bigint | Video | Reached 75% (`TOTAL_VIDEO_P75_COMBINED`). |
| `video_completions` | bigint | Video | Completed views (`TOTAL_VIDEO_P100_COMPLETE`). |
| `ingested_at` | timestamp | Audit | UTC timestamp of the run that wrote the row. |

### Breakdown columns

| Column | Data type | Category | Description | Table |
|---|---|---|---|---|
| `dma_code` | string | Breakdown | Nielsen DMA code. Join to `pinterest_dma_reference.dma_code`. | `pinterest_ad_dma_performance` |
| `gender` | string | Breakdown | Gender value, e.g. `female`, `male`, `unknown`. | `pinterest_ad_gender_performance` |
| `age_bucket` | string | Breakdown | Age bucket, e.g. `45-49`. | `pinterest_ad_age_performance` |

### Reference table — `pinterest_dma_reference`

| Column | Data type | Category | Description |
|---|---|---|---|
| `dma_code` | string | Identifier | Nielsen DMA code. |
| `dma_name` | string | Dimension | Human-readable market name. |
| `ingested_at` | timestamp | Audit | UTC timestamp of the run that wrote the row. |

### Dimension tables

`pinterest_campaign_dim`, `pinterest_ad_group_dim`, and `pinterest_ad_dim` carry the full current-state object returned by Pinterest's entity listing endpoints. Every scalar field becomes its own typed column; every nested object or list field (targeting specs, creative details, tracking parameters) is serialized to a JSON string column, since those shapes vary by objective and ad type and a rigid schema would break the moment Pinterest returns a variant it was not built from. Query them with Spark `from_json` / `get_json_object` or Athena `json_extract`.

All three carry `ad_account_id` and `ingested_at` on the same terms as the fact tables.

---

## Glue Job Walkthrough

### Job parameters

Set at the Glue job level for every job:

```
--datalake-formats iceberg
--additional-python-modules requests>=2.31.0
--extra-py-files s3://<your-bucket>/pinterest_common.zip
```

Job arguments:

| Parameter | Required | Default | Notes |
|---|---|---|---|
| `--JOB_NAME` | Yes | — | Supplied automatically by Glue |
| `--SECRET_NAME` | Yes | — | Secrets Manager secret holding `client_id`, `client_secret`, `refresh_token` |
| `--AWS_REGION` | Yes | — | e.g. `us-east-1` |
| `--ICEBERG_CATALOG` | Yes | — | Glue Data Catalog name registered as an Iceberg catalog |
| `--ICEBERG_DATABASE` | Yes | — | Target database |
| `--ICEBERG_TABLE` | Yes* | — | Target table. *The dimensions job instead takes `ICEBERG_TABLE_CAMPAIGNS`, `ICEBERG_TABLE_AD_GROUPS`, `ICEBERG_TABLE_ADS`. |
| `--ICEBERG_WAREHOUSE_PATH` | Yes | — | `s3://bucket/prefix` for the Iceberg warehouse |
| `--AD_ACCOUNT_IDS` | No | *all discovered accounts* | Comma-separated allowlist; filters the discovered set |
| `--START_DATE` | No | *computed from lookback* | `YYYY-MM-DD`, inclusive. Requires `END_DATE`. |
| `--END_DATE` | No | *computed from lookback* | `YYYY-MM-DD`, inclusive. Requires `START_DATE`. |
| `--LOOKBACK_DAYS` | No | **14** | Rolling window width. Ignored when `START_DATE`/`END_DATE` are set. |

The dimensions job accepts only `AD_ACCOUNT_IDS`; the DMA reference job accepts no optional arguments.

### Execution walkthrough

Using `pinterest_ads_to_iceberg_glue_job.py` (ad-level performance) as the representative example.

1. **Resolve arguments.** `getResolvedOptions` reads required arguments; optional ones are resolved only if actually present in `sys.argv`, since Glue's resolver errors on absent parameters.

2. **Initialize Spark and Glue.** Create the `SparkContext` and `GlueContext`, then configure the Iceberg catalog on the Spark session — `SparkCatalog` implementation, the S3 warehouse path, `GlueCatalog` as the catalog implementation, and `S3FileIO` for I/O. Initialize the Glue `Job` object with the job name and arguments (bookmark/run bookkeeping).

3. **Authenticate.** Read the OAuth credentials from Secrets Manager, exchange the refresh token for a fresh access token, and write the refresh token back if Pinterest rotated it.

4. **Discover ad accounts.** Call `GET /v5/ad_accounts`, paginating fully. If `AD_ACCOUNT_IDS` was supplied, filter the discovered set to that allowlist and warn about any requested account the token cannot see. If nothing remains, commit and exit cleanly.

5. **Resolve the date range.** Either use the explicit `START_DATE`/`END_DATE`, or compute the rolling `LOOKBACK_DAYS` window ending yesterday. Capture a single `ingested_at` UTC timestamp for the whole run.

6. **List entity IDs per account.** For each account, page through the ads listing endpoint collecting ad IDs only. An account with no ads is skipped.

7. **Fetch analytics in batches.** Chunk the ad IDs into batches of 100 (the endpoint's documented cap) and call `/ads/analytics` for each batch with the date range, `granularity=DAY`, and the metric column list. Every call goes through the shared retry/backoff wrapper.

8. **Build rows.** Flatten each returned record into a tuple matching the Spark schema exactly — identifiers, the record's own `DATE` as `stat_date`, each metric cast to its target type, and the run's `ingested_at`.

9. **Short-circuit on empty.** If no rows came back for the window, log it, commit the job, and exit without touching the table — an empty result must not be mistaken for "delete everything."

10. **Create the Spark DataFrame.** Build it from the row tuples against the explicit `StructType` schema, rather than letting Spark infer types from the data.

11. **Write to Iceberg.** Register the DataFrame as a temp view, issue `CREATE TABLE IF NOT EXISTS` with the full column DDL and `PARTITIONED BY (days(stat_date))`, then `MERGE INTO` the target on the key columns — `WHEN MATCHED THEN UPDATE SET *`, `WHEN NOT MATCHED THEN INSERT *`. `stat_date` is cast from string to `date` in the merge's select list.

12. **Commit.** Call `job.commit()` to close out the Glue job run.

### How the other job shapes differ

- **Campaign and ad group jobs** are identical in structure, differing only in the entity listed, the analytics path, the ID batch cap (250), the extra name/status/objective columns, and the merge key.
- **Breakdown jobs (DMA, gender, age)** call `targeting_analytics` with a `targeting_types` value instead of `analytics`, and read the breakdown value from each row's `targeting_value` field into its own column, which also joins the merge key.
- **The dimensions job** skips the date range entirely, pulls full entity objects for all three levels, and writes three tables with `CREATE OR REPLACE TABLE` at the very end of the run — so a mid-run failure leaves the existing tables untouched rather than partially overwritten.
- **The DMA reference job** makes a single call to `/v5/resources/targeting/LOCATION`, needs no ad account context at all, and fully replaces its one small lookup table.
