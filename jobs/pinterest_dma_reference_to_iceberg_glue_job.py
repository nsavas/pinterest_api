"""
AWS Glue job: pull Pinterest's DMA (Designated Market Area) code -> name
lookup and write it to a small Iceberg reference table in S3.

Depends on the flat .py modules in ../common/ -- see ../README.md for how
they're packaged (no wrapping package folder -- see the README for why) and
attached via --extra-py-files.

This table exists to resolve the bare dma_code values written by
pinterest_ads_dma_to_iceberg_glue_job.py into human-readable names -- join on
dma_code. It's global reference data (Pinterest's targeting-option lookup
isn't ad-account-scoped), not per-account time-series data, so this job is
structurally much simpler than the other four: no account discovery, no date
range, no batching.

Glue job parameters expected (set as job arguments):

  --JOB_NAME                 (provided automatically by Glue)
  --SECRET_NAME               Secrets Manager secret name/ARN holding Pinterest
                               OAuth credentials, as JSON:
                               {"client_id": "...", "client_secret": "...", "refresh_token": "..."}
  --AWS_REGION                e.g. us-east-1
  --ICEBERG_CATALOG           Glue Data Catalog name registered as an Iceberg catalog, e.g. "glue_catalog"
  --ICEBERG_DATABASE          target database name, e.g. "marketing"
  --ICEBERG_TABLE             target table name, e.g. "pinterest_dma_reference"
  --ICEBERG_WAREHOUSE_PATH    s3://bucket/prefix for the Iceberg warehouse

Also pass, at the job level (not in this script):
  --datalake-formats iceberg
  --additional-python-modules requests>=2.31.0
  --extra-py-files s3://<your-bucket>/pinterest_common.zip

This script is written for Glue 4.0+ (Spark 3.3+, native Iceberg support).

Design notes:
- GET /resources/targeting/LOCATION returns the full current code -> name
  mapping in one call -- no pagination, no per-account looping (ad_account_id
  is an optional filter on this endpoint, not required). Verified against
  Pinterest's published v5 OpenAPI spec
  (https://github.com/pinterest/api-description/blob/main/v5/openapi.yaml)
  on 2026-08-18; its documented sample response covers both US DMA codes
  (e.g. "811": "U.S.: Reno") and the equivalent geographic breakdown for
  other countries (e.g. "36313": "Australia: Moreton Bay - North").
- Full CREATE OR REPLACE TABLE on every run, not an upsert. DMA boundaries
  are effectively static but can be retired/renamed; a MERGE-based upsert
  would only ever add or update rows, never remove a retired code, so this
  table would silently accumulate stale entries over time. A full replace
  keeps it exactly matching whatever Pinterest returns on each run.
- Not a time-series table (no stat_date, no incremental window), so it
  doesn't need the daily-schedule cadence the other four jobs use -- running
  this weekly or monthly is plenty, since DMA definitions rarely change.
"""

import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "common"))

from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from pinterest_auth import get_secret, refresh_access_token
from pinterest_glue_args import resolve_args
from pinterest_iceberg import replace_table
from pinterest_targeting import fetch_targeting_options

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pinterest_dma_reference_to_iceberg")

TARGETING_TYPE = "LOCATION"  # Pinterest's DMA-level geo breakdown

SCHEMA = StructType([
    StructField("dma_code", StringType(), False),
    StructField("dma_name", StringType(), False),
    StructField("ingested_at", TimestampType(), False),
])

ICEBERG_COLUMNS = [
    ("dma_code", "string"),
    ("dma_name", "string"),
    ("ingested_at", "timestamp"),
]

REQUIRED_ARGS = [
    "JOB_NAME",
    "SECRET_NAME",
    "AWS_REGION",
    "ICEBERG_CATALOG",
    "ICEBERG_DATABASE",
    "ICEBERG_TABLE",
    "ICEBERG_WAREHOUSE_PATH",
]
OPTIONAL_ARGS = []


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
    options = fetch_targeting_options(TARGETING_TYPE, access_token)
    logger.info("Fetched %d %s reference values", len(options), TARGETING_TYPE)

    if not options:
        logger.warning("Pinterest returned no %s reference values, leaving %s untouched",
                        TARGETING_TYPE, full_table_name)
        job.commit()
        return

    ingested_at = datetime.now(timezone.utc)
    rows = [(code, name, ingested_at) for code, name in options.items()]

    df = spark.createDataFrame(rows, schema=SCHEMA)
    replace_table(spark, df, full_table_name, ICEBERG_COLUMNS, temp_view_name="pinterest_dma_reference_source")

    job.commit()


if __name__ == "__main__":
    main()
