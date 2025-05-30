import sys
import json
import urllib.request
import urllib.error
import traceback
import boto3

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import udf, col, current_timestamp
from pyspark.sql.types import StringType

def send_sns_message(sns_topic_arn, message):
    sns = boto3.client("sns")
    sns.publish(
        TopicArn=sns_topic_arn,
        Subject="Weather Gov Glue Job Failure",
        Message=message
    )

try:
    # Obtain arguments, now including SNS_TOPIC_ARN
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'SOURCE_S3_PATH',
        'TARGET_S3_PATH',
        'SNS_TOPIC_ARN'
    ])
    source_s3_path = args['SOURCE_S3_PATH']
    target_s3_path = args['TARGET_S3_PATH']
    sns_topic_arn = args['SNS_TOPIC_ARN']

    # AWS Glue setup
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    logger = glueContext.get_logger()
    logger.info(f"Job started. Reading data from: {source_s3_path}")
    logger.info(f"Data will be written to: {target_s3_path}")
    logger.info(f"SNS Topic ARN will be read from parameter: {sns_topic_arn}")

    # Function to call Weather.gov API
    def fetch_weather_alerts(lat, lon):
        try:
            logger.info(f"Fetching weather alerts for lat={lat}, lon={lon}")
            url = f"https://api.weather.gov/alerts/active?point={lat},{lon}"
            headers = {"User-Agent": "MyWeatherApp (johndauphine@hotmail.com)"}
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=10) as response:
                if response.status == 200:
                    data = json.loads(response.read().decode('utf-8'))
                    alerts = data.get('features', [])
                    if not alerts:
                        logger.info("No alerts returned by API")
                        return "No alerts"
                    logger.info(f"{len(alerts)} alert(s) found")
                    return "; ".join([
                        f"{alert.get('properties', {}).get('headline', 'No headline')} "
                        f"(sent: {alert.get('properties', {}).get('sent', 'N/A')})"
                        for alert in alerts
                    ])
                else:
                    logger.warning(f"Non-200 response: HTTP {response.status}")
                    return f"Error: HTTP {response.status}"
        except urllib.error.HTTPError as e:
            logger.error(f"HTTPError: {e.code} {e.reason}", exc_info=True)
            return f"HTTPError: {e.code} {e.reason}"
        except urllib.error.URLError as e:
            logger.error(f"URLError: {e.reason}", exc_info=True)
            return f"URLError: {e.reason}"
        except Exception as e:
            logger.error(f"Exception occurred while fetching weather alerts: {str(e)}", exc_info=True)
            return f"Exception: {str(e)}"

    # Register UDF
    fetch_weather_alerts_udf = udf(fetch_weather_alerts, StringType())

    logger.info("Reading input CSV from S3...")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(source_s3_path)
    logger.info("Successfully read input CSV.")

    # Enrich with weather alerts and timestamp
    logger.info("Enriching data with weather alerts...")
    df_with_alerts = df.withColumn("alerts", fetch_weather_alerts_udf(col("latitude"), col("longitude")))
    df_with_alerts = df_with_alerts.withColumn("queried_at", current_timestamp())

    logger.info("Showing a preview of the enriched data (truncate=False).")
    df_with_alerts.show(truncate=False)

    logger.info(f"Writing enriched data to Parquet at {target_s3_path}...")
    df_with_alerts.write.mode("append").parquet(target_s3_path)
    logger.info("Write completed successfully.")

except Exception as e:
    error_message = f"Unhandled error: {str(e)}\n\nTraceback:\n{traceback.format_exc()}"
    logger.error(error_message, exc_info=True)

    try:
        # Use the parameter to send the SNS message
        send_sns_message(sns_topic_arn, error_message)
    except Exception as sns_ex:
        logger.error(f"Failed to send SNS alert: {str(sns_ex)}", exc_info=True)

    sys.exit(1)


