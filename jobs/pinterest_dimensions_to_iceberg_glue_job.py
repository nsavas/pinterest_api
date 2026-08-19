"""
AWS Glue job: pull the full Campaign, Ad Group, and Ad dimension/metadata
objects from the Pinterest Ads API (v5) -- name, status, budget, targeting,
creative settings, etc. -- and write them to three Iceberg tables in S3.
No performance metrics here; see pinterest_ads_to_iceberg_glue_job.py,
pinterest_campaigns_to_iceberg_glue_job.py, and
pinterest_ad_groups_to_iceberg_glue_job.py for those.

Depends on the flat .py modules in ../common/ -- see ../README.md for how
they're packaged and attached via --extra-py-files.

Glue job parameters expected (set as job arguments):

  --JOB_NAME                     (provided automatically by Glue)
  --SECRET_NAME                   Secrets Manager secret name/ARN holding Pinterest
                                   OAuth credentials, as JSON:
                                   {"client_id": "...", "client_secret": "...", "refresh_token": "..."}
  --AWS_REGION                    e.g. us-east-1
  --ICEBERG_CATALOG               Glue Data Catalog name registered as an Iceberg catalog, e.g. "glue_catalog"
  --ICEBERG_DATABASE              target database name, e.g. "marketing"
  --ICEBERG_WAREHOUSE_PATH        s3://bucket/prefix for the Iceberg warehouse
  --ICEBERG_TABLE_CAMPAIGNS       target table for campaign dimensions, e.g. "pinterest_campaign_dim"
  --ICEBERG_TABLE_AD_GROUPS       target table for ad group dimensions, e.g. "pinterest_ad_group_dim"
  --ICEBERG_TABLE_ADS             target table for ad dimensions, e.g. "pinterest_ad_dim"

Optional job parameters:

  --AD_ACCOUNT_IDS                comma-separated Pinterest ad account IDs. If omitted (the
                                   normal case), the job calls GET /ad_accounts and pulls
                                   every account the token can see. Pass this only to
                                   restrict a run to a subset of accounts (e.g. testing).

There's no START_DATE/END_DATE/LOOKBACK_DAYS here -- unlike the performance
jobs, this isn't time-series data. Each run is a full current-state snapshot.

Also pass, at the job level (not in this script):
  --datalake-formats iceberg
  --additional-python-modules requests>=2.31.0
  --extra-py-files s3://<your-bucket>/pinterest_common.zip

This script is written for Glue 4.0+ (Spark 3.3+, native Iceberg support).

Design notes:
- Same account-discovery pattern as every other job: list every ad account
  first (GET /ad_accounts, or the AD_ACCOUNT_IDS allowlist), then for each
  account pull every campaign/ad group/ad. The campaigns/ad_groups/ads list
  endpoints return the full entity object already -- unlike the analytics
  endpoints, there's no separate "list IDs, then fetch details" step and no
  `columns` selector, because these endpoints don't support one. Filtering
  isn't needed either: all three list endpoints return every entity in the
  account when called with no campaign_ids/ad_group_ids/ad_ids filter.
- Full CREATE OR REPLACE TABLE per entity type on every run, not an upsert
  (see pinterest_iceberg.py's replace_table(), also used by the DMA
  reference job). A MERGE-based upsert only ever adds/updates rows; it would
  never remove a campaign/ad group/ad that's since been deleted on
  Pinterest's side, so the table would accumulate stale rows forever. All
  three tables are only written once, at the very end, after every account
  has been fetched -- so a mid-run failure leaves the existing tables
  completely untouched rather than partially overwritten.
- Every scalar field (string/int/bool/number) each entity schema defines
  becomes its own typed column. Every field whose value is a nested object
  or an array (e.g. `targeting_spec`, `tracking_urls`, `rejected_reasons`,
  `carting_products`) is serialized to a JSON string column instead of a
  native Spark struct/array column. This is deliberate, not a shortcut:
  Pinterest's nested ad-config objects vary shape by campaign objective and
  ad/creative type (e.g. `targeting_spec` for a Performance+ campaign looks
  nothing like one for a keyword-targeted campaign), so a rigid Spark
  StructType would either reject rows or silently null out fields the
  moment Pinterest returns a shape it wasn't built from. A JSON string
  column preserves every field losslessly; query it with Spark's
  `from_json`/`get_json_object` or Athena's `json_extract` as needed.
- created_time/updated_time/start_time/end_time are converted from Pinterest's
  raw Unix-seconds integers to proper Spark timestamps, since that's an
  unambiguous 1:1 transform (unlike the nested-object fields above) and
  makes the table immediately usable for date filtering without every
  downstream query re-doing the conversion.
- All three entity schemas (Ad, AdGroup via AdGroupBase, Campaign) and every
  field's exact type were verified against Pinterest's published v5 OpenAPI
  spec (https://github.com/pinterest/api-description/blob/main/v5/openapi.yaml)
  on 2026-08-18 -- including the two easy to get wrong: `customer_segment_id`
  is a numeric *string* (Pinterest.Lib.IntegerFormatType), not an integer,
  and `dca_assets` has no declared type in the spec at all (untyped/free-form),
  which is exactly why it's JSON-serialized rather than assumed to be an object.
"""

import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "common"))

from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pinterest_accounts import list_entities, resolve_ad_account_ids
from pinterest_auth import get_secret, refresh_access_token
from pinterest_glue_args import resolve_args
from pinterest_iceberg import replace_table

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pinterest_dimensions_to_iceberg")


# --------------------------------------------------------------------------
# Shared field-conversion helpers
# --------------------------------------------------------------------------

def unix_to_datetime(value):
    """Convert a Pinterest Unix-seconds timestamp field to a UTC datetime."""
    return datetime.fromtimestamp(value, tz=timezone.utc) if value is not None else None


def to_json(value):
    """Serialize a nested object/array field to a JSON string, preserving
    every field losslessly regardless of shape. None stays None."""
    return json.dumps(value) if value is not None else None


# --------------------------------------------------------------------------
# Campaign dimensions
# --------------------------------------------------------------------------

CAMPAIGN_SCHEMA = StructType([
    StructField("campaign_id", StringType(), False),
    StructField("ad_account_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("objective_type", StringType(), True),
    StructField("status", StringType(), True),
    StructField("summary_status", StringType(), True),
    StructField("intended_promotion_type", StringType(), True),
    StructField("type", StringType(), True),
    StructField("order_line_id", StringType(), True),
    StructField("daily_spend_cap", LongType(), True),
    StructField("lifetime_spend_cap", LongType(), True),
    StructField("default_ad_group_budget_in_micro_currency", LongType(), True),
    StructField("is_automated_campaign", BooleanType(), True),
    StructField("is_campaign_budget_optimization", BooleanType(), True),
    StructField("is_carting", BooleanType(), True),
    StructField("is_flexible_daily_budgets", BooleanType(), True),
    StructField("is_ltv_optimized", BooleanType(), True),
    StructField("is_performance_plus", BooleanType(), True),
    StructField("is_top_of_search", BooleanType(), True),
    StructField("created_time", TimestampType(), True),
    StructField("updated_time", TimestampType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    # Nested objects, preserved as JSON -- see module docstring.
    StructField("bid_options_json", StringType(), True),
    StructField("performance_plus_campaign_settings_json", StringType(), True),
    StructField("tracking_urls_json", StringType(), True),
    StructField("ingested_at", TimestampType(), False),
])

CAMPAIGN_ICEBERG_COLUMNS = [
    ("campaign_id", "string"),
    ("ad_account_id", "string"),
    ("name", "string"),
    ("objective_type", "string"),
    ("status", "string"),
    ("summary_status", "string"),
    ("intended_promotion_type", "string"),
    ("type", "string"),
    ("order_line_id", "string"),
    ("daily_spend_cap", "bigint"),
    ("lifetime_spend_cap", "bigint"),
    ("default_ad_group_budget_in_micro_currency", "bigint"),
    ("is_automated_campaign", "boolean"),
    ("is_campaign_budget_optimization", "boolean"),
    ("is_carting", "boolean"),
    ("is_flexible_daily_budgets", "boolean"),
    ("is_ltv_optimized", "boolean"),
    ("is_performance_plus", "boolean"),
    ("is_top_of_search", "boolean"),
    ("created_time", "timestamp"),
    ("updated_time", "timestamp"),
    ("start_time", "timestamp"),
    ("end_time", "timestamp"),
    ("bid_options_json", "string"),
    ("performance_plus_campaign_settings_json", "string"),
    ("tracking_urls_json", "string"),
    ("ingested_at", "timestamp"),
]
CAMPAIGN_KEY_COLUMNS = ["campaign_id"]


def campaign_to_row(ad_account_id: str, c: dict, ingested_at: datetime) -> tuple:
    return (
        c.get("id"),
        ad_account_id,
        c.get("name"),
        c.get("objective_type"),
        c.get("status"),
        c.get("summary_status"),
        c.get("intended_promotion_type"),
        c.get("type"),
        c.get("order_line_id"),
        c.get("daily_spend_cap"),
        c.get("lifetime_spend_cap"),
        c.get("default_ad_group_budget_in_micro_currency"),
        c.get("is_automated_campaign"),
        c.get("is_campaign_budget_optimization"),
        c.get("is_carting"),
        c.get("is_flexible_daily_budgets"),
        c.get("is_ltv_optimized"),
        c.get("is_performance_plus"),
        c.get("is_top_of_search"),
        unix_to_datetime(c.get("created_time")),
        unix_to_datetime(c.get("updated_time")),
        unix_to_datetime(c.get("start_time")),
        unix_to_datetime(c.get("end_time")),
        to_json(c.get("bid_options")),
        to_json(c.get("performance_plus_campaign_settings")),
        to_json(c.get("tracking_urls")),
        ingested_at,
    )


# --------------------------------------------------------------------------
# Ad group dimensions
# --------------------------------------------------------------------------

AD_GROUP_SCHEMA = StructType([
    StructField("ad_group_id", StringType(), False),
    StructField("ad_account_id", StringType(), False),
    StructField("campaign_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("summary_status", StringType(), True),
    StructField("type", StringType(), True),
    StructField("billable_event", StringType(), True),
    StructField("bid_strategy_type", StringType(), True),
    StructField("conversion_learning_mode_type", StringType(), True),
    StructField("budget_type", StringType(), True),
    StructField("pacing_delivery_type", StringType(), True),
    StructField("placement_group", StringType(), True),
    StructField("placement_traffic_type", StringType(), True),
    StructField("promotion_application_level", StringType(), True),
    StructField("promotion_id", StringType(), True),
    StructField("feed_profile_id", StringType(), True),
    StructField("customer_segment_id", StringType(), True),  # numeric string, not an int -- see docstring
    StructField("bid_in_micro_currency", LongType(), True),
    StructField("budget_in_micro_currency", LongType(), True),
    StructField("lifetime_frequency_cap", LongType(), True),
    StructField("local_inventory_radius_in_miles", DoubleType(), True),
    StructField("bid_multiplier", DoubleType(), True),
    StructField("auto_targeting_enabled", BooleanType(), True),
    StructField("is_creative_optimization", BooleanType(), True),
    StructField("is_local_inventory", BooleanType(), True),
    StructField("created_time", TimestampType(), True),
    StructField("updated_time", TimestampType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    # Nested objects/arrays, preserved as JSON -- see module docstring.
    StructField("promotion_ids_json", StringType(), True),
    StructField("targeting_template_ids_json", StringType(), True),
    StructField("dca_assets_json", StringType(), True),
    StructField("ext_features_json", StringType(), True),
    StructField("optimization_goal_metadata_json", StringType(), True),
    StructField("performance_plus_campaign_settings_json", StringType(), True),
    StructField("targeting_spec_json", StringType(), True),
    StructField("tracking_urls_json", StringType(), True),
    StructField("ingested_at", TimestampType(), False),
])

AD_GROUP_ICEBERG_COLUMNS = [
    ("ad_group_id", "string"),
    ("ad_account_id", "string"),
    ("campaign_id", "string"),
    ("name", "string"),
    ("status", "string"),
    ("summary_status", "string"),
    ("type", "string"),
    ("billable_event", "string"),
    ("bid_strategy_type", "string"),
    ("conversion_learning_mode_type", "string"),
    ("budget_type", "string"),
    ("pacing_delivery_type", "string"),
    ("placement_group", "string"),
    ("placement_traffic_type", "string"),
    ("promotion_application_level", "string"),
    ("promotion_id", "string"),
    ("feed_profile_id", "string"),
    ("customer_segment_id", "string"),
    ("bid_in_micro_currency", "bigint"),
    ("budget_in_micro_currency", "bigint"),
    ("lifetime_frequency_cap", "bigint"),
    ("local_inventory_radius_in_miles", "double"),
    ("bid_multiplier", "double"),
    ("auto_targeting_enabled", "boolean"),
    ("is_creative_optimization", "boolean"),
    ("is_local_inventory", "boolean"),
    ("created_time", "timestamp"),
    ("updated_time", "timestamp"),
    ("start_time", "timestamp"),
    ("end_time", "timestamp"),
    ("promotion_ids_json", "string"),
    ("targeting_template_ids_json", "string"),
    ("dca_assets_json", "string"),
    ("ext_features_json", "string"),
    ("optimization_goal_metadata_json", "string"),
    ("performance_plus_campaign_settings_json", "string"),
    ("targeting_spec_json", "string"),
    ("tracking_urls_json", "string"),
    ("ingested_at", "timestamp"),
]
AD_GROUP_KEY_COLUMNS = ["ad_group_id"]


def ad_group_to_row(ad_account_id: str, ag: dict, ingested_at: datetime) -> tuple:
    return (
        ag.get("id"),
        ad_account_id,
        ag.get("campaign_id"),
        ag.get("name"),
        ag.get("status"),
        ag.get("summary_status"),
        ag.get("type"),
        ag.get("billable_event"),
        ag.get("bid_strategy_type"),
        ag.get("conversion_learning_mode_type"),
        ag.get("budget_type"),
        ag.get("pacing_delivery_type"),
        ag.get("placement_group"),
        ag.get("placement_traffic_type"),
        ag.get("promotion_application_level"),
        ag.get("promotion_id"),
        ag.get("feed_profile_id"),
        ag.get("customer_segment_id"),
        ag.get("bid_in_micro_currency"),
        ag.get("budget_in_micro_currency"),
        ag.get("lifetime_frequency_cap"),
        ag.get("local_inventory_radius_in_miles"),
        ag.get("bid_multiplier"),
        ag.get("auto_targeting_enabled"),
        ag.get("is_creative_optimization"),
        ag.get("is_local_inventory"),
        unix_to_datetime(ag.get("created_time")),
        unix_to_datetime(ag.get("updated_time")),
        unix_to_datetime(ag.get("start_time")),
        unix_to_datetime(ag.get("end_time")),
        to_json(ag.get("promotion_ids")),
        to_json(ag.get("targeting_template_ids")),
        to_json(ag.get("dca_assets")),
        to_json(ag.get("ext_features")),
        to_json(ag.get("optimization_goal_metadata")),
        to_json(ag.get("performance_plus_campaign_settings")),
        to_json(ag.get("targeting_spec")),
        to_json(ag.get("tracking_urls")),
        ingested_at,
    )


# --------------------------------------------------------------------------
# Ad dimensions
# --------------------------------------------------------------------------

AD_SCHEMA = StructType([
    StructField("ad_id", StringType(), False),
    StructField("ad_account_id", StringType(), False),
    StructField("ad_group_id", StringType(), True),
    StructField("campaign_id", StringType(), True),
    StructField("pin_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("summary_status", StringType(), True),
    StructField("review_status", StringType(), True),
    StructField("type", StringType(), True),
    StructField("creative_type", StringType(), True),
    StructField("customizable_cta_type", StringType(), True),
    StructField("disclosure_type", StringType(), True),
    StructField("grid_click_type", StringType(), True),
    StructField("carting_platform_type", StringType(), True),
    StructField("collections_header_type", StringType(), True),
    StructField("lead_form_id", StringType(), True),
    StructField("android_deep_link", StringType(), True),
    StructField("ios_deep_link", StringType(), True),
    StructField("destination_url", StringType(), True),
    StructField("click_tracking_url", StringType(), True),
    StructField("view_tracking_url", StringType(), True),
    StructField("disclosure_url", StringType(), True),
    StructField("collection_items_destination_url_template", StringType(), True),
    StructField("is_carting", BooleanType(), True),
    StructField("is_collage_accepted_terms", BooleanType(), True),
    StructField("is_collage_single_destination", BooleanType(), True),
    StructField("is_pin_deleted", BooleanType(), True),
    StructField("is_removable", BooleanType(), True),
    StructField("created_time", TimestampType(), True),
    StructField("updated_time", TimestampType(), True),
    # Nested objects/arrays, preserved as JSON -- see module docstring.
    StructField("carousel_android_deep_links_json", StringType(), True),
    StructField("carousel_destination_urls_json", StringType(), True),
    StructField("carousel_ios_deep_links_json", StringType(), True),
    StructField("carting_products_json", StringType(), True),
    StructField("quiz_pin_data_json", StringType(), True),
    StructField("rejected_reasons_json", StringType(), True),
    StructField("rejection_labels_json", StringType(), True),
    StructField("tracking_urls_json", StringType(), True),
    StructField("ingested_at", TimestampType(), False),
])

AD_ICEBERG_COLUMNS = [
    ("ad_id", "string"),
    ("ad_account_id", "string"),
    ("ad_group_id", "string"),
    ("campaign_id", "string"),
    ("pin_id", "string"),
    ("name", "string"),
    ("status", "string"),
    ("summary_status", "string"),
    ("review_status", "string"),
    ("type", "string"),
    ("creative_type", "string"),
    ("customizable_cta_type", "string"),
    ("disclosure_type", "string"),
    ("grid_click_type", "string"),
    ("carting_platform_type", "string"),
    ("collections_header_type", "string"),
    ("lead_form_id", "string"),
    ("android_deep_link", "string"),
    ("ios_deep_link", "string"),
    ("destination_url", "string"),
    ("click_tracking_url", "string"),
    ("view_tracking_url", "string"),
    ("disclosure_url", "string"),
    ("collection_items_destination_url_template", "string"),
    ("is_carting", "boolean"),
    ("is_collage_accepted_terms", "boolean"),
    ("is_collage_single_destination", "boolean"),
    ("is_pin_deleted", "boolean"),
    ("is_removable", "boolean"),
    ("created_time", "timestamp"),
    ("updated_time", "timestamp"),
    ("carousel_android_deep_links_json", "string"),
    ("carousel_destination_urls_json", "string"),
    ("carousel_ios_deep_links_json", "string"),
    ("carting_products_json", "string"),
    ("quiz_pin_data_json", "string"),
    ("rejected_reasons_json", "string"),
    ("rejection_labels_json", "string"),
    ("tracking_urls_json", "string"),
    ("ingested_at", "timestamp"),
]
AD_KEY_COLUMNS = ["ad_id"]


def ad_to_row(ad_account_id: str, a: dict, ingested_at: datetime) -> tuple:
    return (
        a.get("id"),
        ad_account_id,
        a.get("ad_group_id"),
        a.get("campaign_id"),
        a.get("pin_id"),
        a.get("name"),
        a.get("status"),
        a.get("summary_status"),
        a.get("review_status"),
        a.get("type"),
        a.get("creative_type"),
        a.get("customizable_cta_type"),
        a.get("disclosure_type"),
        a.get("grid_click_type"),
        a.get("carting_platform_type"),
        a.get("collections_header_type"),
        a.get("lead_form_id"),
        a.get("android_deep_link"),
        a.get("ios_deep_link"),
        a.get("destination_url"),
        a.get("click_tracking_url"),
        a.get("view_tracking_url"),
        a.get("disclosure_url"),
        a.get("collection_items_destination_url_template"),
        a.get("is_carting"),
        a.get("is_collage_accepted_terms"),
        a.get("is_collage_single_destination"),
        a.get("is_pin_deleted"),
        a.get("is_removable"),
        unix_to_datetime(a.get("created_time")),
        unix_to_datetime(a.get("updated_time")),
        to_json(a.get("carousel_android_deep_links")),
        to_json(a.get("carousel_destination_urls")),
        to_json(a.get("carousel_ios_deep_links")),
        to_json(a.get("carting_products")),
        to_json(a.get("quiz_pin_data")),
        to_json(a.get("rejected_reasons")),
        to_json(a.get("rejection_labels")),
        to_json(a.get("tracking_urls")),
        ingested_at,
    )


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

REQUIRED_ARGS = [
    "JOB_NAME",
    "SECRET_NAME",
    "AWS_REGION",
    "ICEBERG_CATALOG",
    "ICEBERG_DATABASE",
    "ICEBERG_WAREHOUSE_PATH",
    "ICEBERG_TABLE_CAMPAIGNS",
    "ICEBERG_TABLE_AD_GROUPS",
    "ICEBERG_TABLE_ADS",
]
OPTIONAL_ARGS = ["AD_ACCOUNT_IDS"]


def main():
    args = resolve_args(REQUIRED_ARGS, OPTIONAL_ARGS)

    catalog = args["ICEBERG_CATALOG"]
    database = args["ICEBERG_DATABASE"]

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

    logger.info("Pulling Pinterest campaign/ad group/ad dimensions across %d account(s)", len(ad_account_ids))
    ingested_at = datetime.now(timezone.utc)

    campaign_rows, ad_group_rows, ad_rows = [], [], []
    for ad_account_id in ad_account_ids:
        for c in list_entities(ad_account_id, access_token, "campaigns"):
            campaign_rows.append(campaign_to_row(ad_account_id, c, ingested_at))
        for ag in list_entities(ad_account_id, access_token, "ad_groups"):
            ad_group_rows.append(ad_group_to_row(ad_account_id, ag, ingested_at))
        for a in list_entities(ad_account_id, access_token, "ads"):
            ad_rows.append(ad_to_row(ad_account_id, a, ingested_at))

    logger.info("Fetched %d campaign(s), %d ad group(s), %d ad(s) across %d account(s)",
                len(campaign_rows), len(ad_group_rows), len(ad_rows), len(ad_account_ids))

    # -- write -------------------------------------------------------------
    # Each table is only written once, at the end, after every account has
    # been fetched -- so a mid-run failure leaves existing tables untouched
    # rather than partially overwritten. Full replace, not merge/upsert: see
    # module docstring for why (deleted entities must disappear, not linger).
    if campaign_rows:
        df = spark.createDataFrame(campaign_rows, schema=CAMPAIGN_SCHEMA)
        replace_table(spark, df, f"{catalog}.{database}.{args['ICEBERG_TABLE_CAMPAIGNS']}",
                      CAMPAIGN_ICEBERG_COLUMNS, temp_view_name="pinterest_campaigns_dim_source")
    else:
        logger.warning("No campaigns found, leaving %s untouched", args["ICEBERG_TABLE_CAMPAIGNS"])

    if ad_group_rows:
        df = spark.createDataFrame(ad_group_rows, schema=AD_GROUP_SCHEMA)
        replace_table(spark, df, f"{catalog}.{database}.{args['ICEBERG_TABLE_AD_GROUPS']}",
                      AD_GROUP_ICEBERG_COLUMNS, temp_view_name="pinterest_ad_groups_dim_source")
    else:
        logger.warning("No ad groups found, leaving %s untouched", args["ICEBERG_TABLE_AD_GROUPS"])

    if ad_rows:
        df = spark.createDataFrame(ad_rows, schema=AD_SCHEMA)
        replace_table(spark, df, f"{catalog}.{database}.{args['ICEBERG_TABLE_ADS']}",
                      AD_ICEBERG_COLUMNS, temp_view_name="pinterest_ads_dim_source")
    else:
        logger.warning("No ads found, leaving %s untouched", args["ICEBERG_TABLE_ADS"])

    job.commit()


if __name__ == "__main__":
    main()
