import sys
import json
import urllib.request
import urllib.error
import logging
import boto3

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import udf, col, current_timestamp
from pyspark.sql.types import StringType

# AWS Glue setup
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Parameters from Glue job args
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SOURCE_S3_PATH', 'TARGET_S3_PATH', 'SNS_TOPIC_ARN'])
source_s3_path = args['SOURCE_S3_PATH']           # e.g., s3://your-bucket/data/city-data.csv
target_s3_path = args['TARGET_S3_PATH']           # e.g., s3://your-bucket/data/weather_alerts.parquet

# Logger and SNS client setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
sns_client = boto3.client('sns')

# Function to call Weather.gov API
def fetch_weather_alerts(lat, lon):
    logger.info(f"Fetching weather alerts for lat={lat}, lon={lon}")
    try:
        url = f"https://api.weather.gov/alerts/active?point={lat},{lon}"
        headers = {"User-Agent": "MyWeatherApp (johndauphine@hotmail.com)"}
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=10) as response:
            if response.status == 200:
                data = json.loads(response.read().decode('utf-8'))
                alerts = data.get('features', [])
                if not alerts:
                    return "No alerts"
                return "; ".join([
                    f"{alert.get('properties', {}).get('headline', 'No headline')} (sent: {alert.get('properties', {}).get('sent', 'N/A')})"
                    for alert in alerts
                ])
            else:
                return f"Error: HTTP {response.status}"
    except urllib.error.HTTPError as e:
        return f"HTTPError: {e.code} {e.reason}"
    except urllib.error.URLError as e:
        return f"URLError: {e.reason}"
    except Exception as e:
        logger.error(f"Exception encountered: {str(e)}")
        sns_client.publish(
            TopicArn=args['SNS_TOPIC_ARN'],
            Message=f"Error fetching weather alerts: {str(e)}"
        )
        return f"Exception: {str(e)}"

# Register UDF
fetch_weather_alerts_udf = udf(fetch_weather_alerts, StringType())

# Read input CSV from S3
try:
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(source_s3_path)
    logger.info("Data read successfully from source CSV.")
except Exception as e:
    logger.error(f"Error reading data from CSV: {str(e)}")
    sns_client.publish(
        TopicArn=args['SNS_TOPIC_ARN'],
        Message=f"Error reading data from CSV: {str(e)}"
    )
    raise

# Enrich with weather alerts and timestamp
df_with_alerts = df.withColumn("alerts", fetch_weather_alerts_udf(col("latitude"), col("longitude")))
df_with_alerts = df_with_alerts.withColumn("queried_at", current_timestamp())

# Show preview (optional)
df_with_alerts.show(truncate=False)

# Write to S3 in Parquet format
try:
    df_with_alerts.write.mode("append").parquet(target_s3_path)
    logger.info("Data written to S3 in Parquet format.")
except Exception as e:
    logger.error(f"Error writing data to S3: {str(e)}")
    sns_client.publish(
        TopicArn=args['SNS_TOPIC_ARN'],
        Message=f"Error writing data to S3: {str(e)}"
    )
    raise


