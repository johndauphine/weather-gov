import sys
import os
import traceback
from datetime import date

import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, concat_ws, sha2, lit
from pyspark.sql.utils import AnalysisException
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

def publish_failure(topic_arn: str | None, message: str) -> None:
    """
    Publish a failure message to the provided SNS topic ARN.
    """
    # We'll use the Glue logger, so we need a reference below. This function
    # is called after the logger is defined, hence the global reference.
    global logger

    if not topic_arn:
        logger.warning("SNS_TOPIC_ARN not set – skipping SNS notification")
        return
    try:
        boto3.client("sns").publish(
            TopicArn=topic_arn,
            Subject="Glue job failure",
            Message=message,
        )
        logger.info("Failure notification sent to SNS")
    except Exception as sns_err:
        logger.error("SNS publish failed: %s", sns_err)

try:
    # Retrieve required parameters
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "SOURCE_PARQUET_PATH",
            "ICEBERG_CATALOG_NAME",
            "ICEBERG_DB",
            "ICEBERG_TABLE",
            "ICEBERG_WAREHOUSE_S3_PATH",
        ],
    )

    # Attempt to get an SNS topic parameter
    sns_topic = (
        next(
            (p.split("=", 1)[1] for p in sys.argv if p.startswith("--SNS_TOPIC_ARN=")),
            None,
        )
        or os.getenv("SNS_TOPIC_ARN")
    )

    src_parquet = args["SOURCE_PARQUET_PATH"].rstrip("/")
    catalog     = args["ICEBERG_CATALOG_NAME"]
    db          = args["ICEBERG_DB"]
    table       = args["ICEBERG_TABLE"]
    warehouse   = args["ICEBERG_WAREHOUSE_S3_PATH"].strip().rstrip("/")

    iceberg_id  = f"{catalog}.{db}.{table}"

    # Create Spark session with Iceberg config
    spark = (
        SparkSession.builder
            .appName(args["JOB_NAME"])
            .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{catalog}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
            .config(f"spark.sql.catalog.{catalog}.warehouse", warehouse)
            .config(f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .getOrCreate()
    )

    # Create the GlueContext and Job
    glue_ctx = GlueContext(spark.sparkContext)
    job = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)

    # Retrieve the AWS Glue logger
    logger = glue_ctx.get_logger()

    # Make sure database exists
    spark.sql(
        f"""
        CREATE DATABASE IF NOT EXISTS {db}
        LOCATION '{warehouse}/{db}.db/'
        """
    )

    logger.info("Reading Parquet from %s", src_parquet)
    df = spark.read.parquet(src_parquet)
    logger.info("Source rows: %d", df.count())

    # If the table has a 'queried_at' column, convert it to event_date; otherwise default to today.
    if "queried_at" in df.columns:
        df = df.withColumn("event_date", to_date(col("queried_at")))
    else:
        df = df.withColumn("event_date", lit(date.today()))
        logger.warning("queried_at missing – event_date defaulted to today")

    # Create a unique record_id
    df = df.withColumn(
        "record_id",
        sha2(concat_ws("||", *[col(c).cast("string") for c in df.columns]), 256)
    )

    # Check if the Iceberg table exists
    try:
        spark.read.table(iceberg_id)
        table_exists = True
        logger.info("Iceberg table exists")
    except AnalysisException:
        table_exists = False
        logger.info("Iceberg table does not exist – first load")

    # Create the table if needed
    if not table_exists:
        df.createOrReplaceTempView("stage_firstload")
        spark.sql(
            f"""
            CREATE TABLE {iceberg_id}
            USING ICEBERG
            PARTITIONED BY (event_date)
            AS
            SELECT * FROM stage_firstload
            """
        )
        logger.info("CTAS complete – inserted %d rows", df.count())
        job.commit()
        sys.exit(0)

    # Latest known event_date in the existing table
    max_date = (
        spark.sql(f"SELECT max(event_date) AS maxd FROM {iceberg_id}")
             .collect()[0]["maxd"]
    )
    logger.info("High‑water‑mark event_date: %s", max_date)

    # Split new vs. overlapping data
    df_new      = df.filter(col("event_date") > lit(max_date))
    df_overlap  = df.filter(col("event_date") <= lit(max_date))

    inserted_new = 0
    if df_new.take(1):
        inserted_new = df_new.count()
        logger.info("Appending %d brand‑new rows", inserted_new)
        df_new.writeTo(iceberg_id).append()

    inserted_overlap = 0
    if df_overlap.take(1):
        existing_ids = spark.read.table(iceberg_id).select("record_id").distinct()
        df_overlap_new = df_overlap.join(existing_ids, on="record_id", how="left_anti")
        if df_overlap_new.take(1):
            inserted_overlap = df_overlap_new.count()
            logger.info("Merging %d late‑arriving unique rows", inserted_overlap)
            df_overlap_new.createOrReplaceTempView("stage_overlap_new")
            spark.sql(
                f"""
                MERGE INTO {iceberg_id} AS target
                USING stage_overlap_new AS source
                ON source.record_id = target.record_id
                WHEN NOT MATCHED THEN INSERT *
                """
            )
        else:
            logger.info("All overlap rows were duplicates – nothing to merge")

    total_inserted = inserted_new + inserted_overlap
    logger.info(
        "Rows inserted this run → append: %d  merge: %d  TOTAL: %d",
        inserted_new, inserted_overlap, total_inserted,
    )

    # Commit the Glue job
    job.commit()
    logger.info("Glue job finished SUCCESSFULLY")

except Exception:
    tb = traceback.format_exc()
    logger.error("Unhandled exception:\n%s", tb)
    publish_failure(os.getenv("SNS_TOPIC_ARN") or sns_topic, tb)
    raise
