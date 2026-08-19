"""
AWS Glue job: pull ad-level performance data broken down by DMA (Designated
Market Area) from the Pinterest Ads API (v5) and upsert it into an Iceberg
table in S3.

Depends on the flat .py modules in ../common/ -- see ../README.md for how
they're packaged (no wrapping package folder -- see the README for why) and
attached via --extra-py-files.

This is a companion to pinterest_ads_to_iceberg_glue_job.py, not a
replacement -- it hits a structurally different endpoint
(ads/targeting_analytics, not ads/analytics) and produces a different grain
of table: one row per (ad, date, DMA) instead of one row per (ad, date).
Query DMA names by joining this table's dma_code column against
pinterest_dma_reference (see pinterest_dma_reference_to_iceberg_glue_job.py).

Glue job parameters expected (set as job arguments):

  --JOB_NAME                 (provided automatically by Glue)
  --SECRET_NAME               Secrets Manager secret name/ARN holding Pinterest
                               OAuth credentials, as JSON:
                               {"client_id": "...", "client_secret": "...", "refresh_token": "..."}
  --AWS_REGION                e.g. us-east-1
  --ICEBERG_CATALOG           Glue Data Catalog name registered as an Iceberg catalog, e.g. "glue_catalog"
  --ICEBERG_DATABASE          target database name, e.g. "marketing"
  --ICEBERG_TABLE             target table name, e.g. "pinterest_ad_dma_performance"
  --ICEBERG_WAREHOUSE_PATH    s3://bucket/prefix for the Iceberg warehouse

Optional job parameters:

  --AD_ACCOUNT_IDS             comma-separated Pinterest ad account IDs. If omitted (the
                                normal case), the job calls GET /ad_accounts and pulls
                                every account the token can see. Pass this only to
                                restrict a run to a subset of accounts (e.g. testing).
  --START_DATE                 YYYY-MM-DD (inclusive). If omitted, computed from LOOKBACK_DAYS.
  --END_DATE                   YYYY-MM-DD (inclusive). If omitted, computed from LOOKBACK_DAYS.
  --LOOKBACK_DAYS               integer, default 14. Ignored if START_DATE/END_DATE are set.

Also pass, at the job level (not in this script):
  --datalake-formats iceberg
  --additional-python-modules requests>=2.31.0
  --extra-py-files s3://<your-bucket>/pinterest_common.zip

This script is written for Glue 4.0+ (Spark 3.3+, native Iceberg support).

Design notes specific to DMA breakdown:
- targeting_types=LOCATION is Pinterest's DMA-level geo breakdown -- verified
  against Pinterest's published v5 OpenAPI spec
  (https://github.com/pinterest/api-description/blob/main/v5/openapi.yaml)
  on 2026-08-18. Its example response shows targeting_value "500" under
  targeting_type LOCATION (a bare Nielsen-style DMA code), versus "US-CA" for
  REGION (state-level) and "US:94102" for GEO (ZIP-level) -- LOCATION is the
  DMA granularity, the other two are coarser/finer and not what this job asks
  for.
- ads/targeting_analytics's response nests the requested identifier/date/
  metric columns inside each row's "metrics" object, alongside sibling
  "targeting_type"/"targeting_value" fields -- structurally different from
  ads/analytics's flat rows. pinterest_targeting.py's fetch_targeting_analytics()
  already unwraps the outer "data" list; this job still has to reach into
  each item's "metrics" dict itself.
- targeting_value ("500") is a bare code with no name attached. Resolving it
  to a human-readable DMA name requires joining against a separate reference
  table -- see pinterest_dma_reference_to_iceberg_glue_job.py.
- Same batching rationale as the base ad-level job: ads/targeting_analytics
  requires ad_ids on every call (max 250 per Pinterest's documented cap), so
  we list ad IDs first, then batch.
"""

import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "common"))

from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pinterest_accounts import list_entity_ids, resolve_ad_account_ids
from pinterest_auth import get_secret, refresh_access_token
from pinterest_dates import chunked, resolve_date_range
from pinterest_glue_args import resolve_args
from pinterest_iceberg import upsert
from pinterest_targeting import fetch_targeting_analytics

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pinterest_ads_dma_to_iceberg")

ENTITY_PATH = "ads"
ID_PARAM_NAME = "ad_ids"
ID_BATCH_SIZE = 250  # Pinterest's documented max ad_ids per targeting_analytics call
TARGETING_TYPE = "LOCATION"  # Pinterest's DMA-level geo breakdown

# Same metric set as pinterest_ads_to_iceberg_glue_job.py's ANALYTICS_COLUMNS,
# deliberately kept identical so summing this table's metrics across DMA for
# a given (ad, date) is comparable to that table's row for the same (ad,
# date) -- modulo any traffic Pinterest doesn't attribute to a DMA.
# Verified against the `ReportingColumnSync` enum in Pinterest's published v5
# OpenAPI spec on 2026-08-18.
ANALYTICS_COLUMNS = [
    "AD_ID",
    "AD_GROUP_ID",
    "CAMPAIGN_ID",
    "AD_ACCOUNT_ID",
    "SPEND_IN_DOLLAR",
    "TOTAL_IMPRESSION",
    "TOTAL_CLICKTHROUGH",
    "TOTAL_ENGAGEMENT",
    "TOTAL_CONVERSIONS",
    "LEADS",
    "COST_PER_LEAD",
    "ECPC_IN_DOLLAR",
    "CPM_IN_DOLLAR",
    "CTR",
    "TOTAL_VIDEO_P0_COMBINED",
    "TOTAL_VIDEO_P25_COMBINED",
    "TOTAL_VIDEO_P50_COMBINED",
    "TOTAL_VIDEO_P75_COMBINED",
    "TOTAL_VIDEO_P100_COMPLETE",
]

SCHEMA = StructType([
    StructField("ad_account_id", StringType(), False),
    StructField("ad_id", StringType(), False),
    StructField("ad_group_id", StringType(), True),
    StructField("campaign_id", StringType(), True),
    StructField("dma_code", StringType(), False),
    StructField("stat_date", StringType(), False),  # cast to date in the Iceberg merge
    StructField("spend", DoubleType(), True),
    StructField("impressions", LongType(), True),
    StructField("clicks", LongType(), True),
    StructField("engagements", LongType(), True),
    StructField("conversions", LongType(), True),
    StructField("leads", LongType(), True),
    StructField("cost_per_lead", DoubleType(), True),
    StructField("ecpc", DoubleType(), True),
    StructField("cpm", DoubleType(), True),
    StructField("ctr", DoubleType(), True),
    StructField("video_p0_combined", LongType(), True),
    StructField("video_p25_combined", LongType(), True),
    StructField("video_p50_combined", LongType(), True),
    StructField("video_p75_combined", LongType(), True),
    StructField("video_completions", LongType(), True),
    StructField("ingested_at", TimestampType(), False),
])

# Iceberg table DDL, in the same order as SCHEMA. stat_date is cast from the
# source string column via a select_expr override.
ICEBERG_COLUMNS = [
    ("ad_account_id", "string"),
    ("ad_id", "string"),
    ("ad_group_id", "string"),
    ("campaign_id", "string"),
    ("dma_code", "string"),
    ("stat_date", "date", "CAST(stat_date AS date)"),
    ("spend", "double"),
    ("impressions", "bigint"),
    ("clicks", "bigint"),
    ("engagements", "bigint"),
    ("conversions", "bigint"),
    ("leads", "bigint"),
    ("cost_per_lead", "double"),
    ("ecpc", "double"),
    ("cpm", "double"),
    ("ctr", "double"),
    ("video_p0_combined", "bigint"),
    ("video_p25_combined", "bigint"),
    ("video_p50_combined", "bigint"),
    ("video_p75_combined", "bigint"),
    ("video_completions", "bigint"),
    ("ingested_at", "timestamp"),
]
KEY_COLUMNS = ["ad_account_id", "ad_id", "dma_code", "stat_date"]
PARTITION_EXPR = "days(stat_date)"

REQUIRED_ARGS = [
    "JOB_NAME",
    "SECRET_NAME",
    "AWS_REGION",
    "ICEBERG_CATALOG",
    "ICEBERG_DATABASE",
    "ICEBERG_TABLE",
    "ICEBERG_WAREHOUSE_PATH",
]
OPTIONAL_ARGS = ["AD_ACCOUNT_IDS", "START_DATE", "END_DATE", "LOOKBACK_DAYS"]


def to_row(ad_account_id: str, dma_code: str, stat_date: str, metrics: dict,
           ingested_at: datetime) -> tuple:
    def num(key, cast=float):
        val = metrics.get(key)
        return cast(val) if val is not None else None

    return (
        ad_account_id,
        metrics.get("AD_ID"),
        metrics.get("AD_GROUP_ID"),
        metrics.get("CAMPAIGN_ID"),
        dma_code,
        stat_date,
        num("SPEND_IN_DOLLAR", float),
        num("TOTAL_IMPRESSION", int),
        num("TOTAL_CLICKTHROUGH", int),
        num("TOTAL_ENGAGEMENT", int),
        num("TOTAL_CONVERSIONS", int),
        num("LEADS", int),
        num("COST_PER_LEAD", float),
        num("ECPC_IN_DOLLAR", float),
        num("CPM_IN_DOLLAR", float),
        num("CTR", float),
        num("TOTAL_VIDEO_P0_COMBINED", int),
        num("TOTAL_VIDEO_P25_COMBINED", int),
        num("TOTAL_VIDEO_P50_COMBINED", int),
        num("TOTAL_VIDEO_P75_COMBINED", int),
        num("TOTAL_VIDEO_P100_COMPLETE", int),
        ingested_at,
    )


def main():
    args = resolve_args(REQUIRED_ARGS, OPTIONAL_ARGS)

    catalog = args["ICEBERG_CATALOG"]
    database = args["ICEBERG_DATABASE"]
    table = args["ICEBERG_TABLE"]
    full_table_name = f"{catalog}.{database}.{table}"

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = (
        glueContext.spark_session.builder
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.warehouse", args["ICEBERG_WAREHOUSE_PATH"])
        .config(f"spark.sql.catalog.{catalog}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .getOrCreate()
    )
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    # -- credentials --------------------------------------------------
    creds = get_secret(args["SECRET_NAME"], args["AWS_REGION"])
    access_token = refresh_access_token(args["SECRET_NAME"], args["AWS_REGION"], creds)

    # -- fetch -----------------------------------------------------------
    ad_account_ids = resolve_ad_account_ids(args, access_token)
    if not ad_account_ids:
        logger.warning("No ad accounts to pull (none visible to this token, or filter matched none)")
        job.commit()
        return

    start_date, end_date = resolve_date_range(args)
    logger.info("Pulling Pinterest ad DMA analytics for %s..%s across %d account(s)",
                start_date, end_date, len(ad_account_ids))
    ingested_at = datetime.now(timezone.utc)

    all_rows = []
    for ad_account_id in ad_account_ids:
        entity_ids = list_entity_ids(ad_account_id, access_token, ENTITY_PATH)
        if not entity_ids:
            logger.info("Ad account %s has no ads, skipping", ad_account_id)
            continue

        for batch in chunked(entity_ids, ID_BATCH_SIZE):
            breakdown_rows = fetch_targeting_analytics(
                ad_account_id, ENTITY_PATH, ID_PARAM_NAME, batch, TARGETING_TYPE,
                start_date, end_date, access_token, ANALYTICS_COLUMNS,
            )
            for item in breakdown_rows:
                dma_code = item.get("targeting_value")
                metrics = item.get("metrics", {})
                stat_date = metrics.get("DATE", start_date)
                all_rows.append(to_row(ad_account_id, dma_code, stat_date, metrics, ingested_at))

    logger.info("Fetched %d ad-DMA-day rows across %d ad account(s)", len(all_rows), len(ad_account_ids))

    if not all_rows:
        logger.info("No data returned for %s..%s, nothing to write", start_date, end_date)
        job.commit()
        return

    df = spark.createDataFrame(all_rows, schema=SCHEMA)
    upsert(spark, df, full_table_name, ICEBERG_COLUMNS, KEY_COLUMNS,
           PARTITION_EXPR, temp_view_name="pinterest_ads_dma_source")

    job.commit()


if __name__ == "__main__":
    main()
