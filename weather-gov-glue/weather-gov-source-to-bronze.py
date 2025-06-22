import json
import urllib.request
import urllib.error
import boto3
from datetime import datetime
import hashlib
from botocore.exceptions import ClientError
import logging
import ssl

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class WeatherAlertsDownloader:
    def __init__(self, bucket_name, aws_profile=None, output_format='json'):
        """
        Initialize the downloader with S3 configuration.
        
        Args:
            bucket_name: S3 bucket name
            aws_profile: AWS profile name (optional, uses default if not specified)
            output_format: Output format - 'json' or 'ndjson' (newline-delimited JSON)
        """
        self.bucket_name = bucket_name
        self.output_format = output_format
        
        # Initialize S3 client
        if aws_profile:
            session = boto3.Session(profile_name=aws_profile)
            self.s3_client = session.client('s3')
        else:
            self.s3_client = boto3.client('s3')
        
        # Weather.gov API endpoint for active alerts
        self.alerts_url = "https://api.weather.gov/alerts/active"
        
        # Headers for weather.gov API
        self.headers = {
            'User-Agent': 'WeatherAlertsDownloader/1.0 (contact@example.com)',
            'Accept': 'application/geo+json'
        }
        
        # Create SSL context
        self.ssl_context = ssl.create_default_context()
    
    def _get_alerts(self):
        """Retrieve alerts from weather.gov API using urllib."""
        try:
            # Create request with headers
            req = urllib.request.Request(self.alerts_url)
            for key, value in self.headers.items():
                req.add_header(key, value)
            
            # Make request with timeout
            with urllib.request.urlopen(req, context=self.ssl_context, timeout=30) as response:
                data = response.read()
                return json.loads(data.decode('utf-8'))
                
        except urllib.error.HTTPError as e:
            logger.error(f"HTTP Error {e.code}: {e.reason}")
            raise
        except urllib.error.URLError as e:
            logger.error(f"URL Error: {e.reason}")
            raise
        except Exception as e:
            logger.error(f"Error fetching alerts: {e}")
            raise
    
    def _generate_s3_key(self, alert_id, alert_time):
        """Generate S3 key with year/month/day/hour partitioning based on alert time."""
        year = alert_time.strftime('%Y')
        month = alert_time.strftime('%m')
        day = alert_time.strftime('%d')
        hour = alert_time.strftime('%H')
        
        # Create a filename that includes the alert ID for idempotency
        # Clean the alert ID to be filesystem-safe
        safe_alert_id = alert_id.replace('/', '_').replace('\\', '_')
        filename = f"alert_{safe_alert_id}.json"
        
        return f"weather-alerts/{year}/{month}/{day}/{hour}/{filename}"
    
    def _parse_alert_time(self, alert):
        """Extract and parse the alert's timestamp for partitioning."""
        properties = alert.get('properties', {})
        
        # Try different time fields in order of preference
        time_fields = ['effective', 'onset', 'sent']
        
        for field in time_fields:
            time_str = properties.get(field)
            if time_str:
                try:
                    # Parse ISO format timestamp (handles timezone)
                    # Example: "2025-06-08T10:00:00-05:00"
                    if 'T' in time_str:
                        # Remove timezone for parsing, then treat as UTC
                        if '+' in time_str or time_str.count('-') > 2:
                            # Has timezone
                            if '+' in time_str:
                                time_str = time_str.split('+')[0]
                            else:
                                # Handle negative timezone (e.g., -05:00)
                                parts = time_str.rsplit('-', 1)
                                if len(parts) == 2 and ':' in parts[1]:
                                    time_str = parts[0]
                        
                        # Parse the datetime
                        dt = datetime.strptime(time_str.replace('Z', ''), '%Y-%m-%dT%H:%M:%S')
                        return dt
                except Exception as e:
                    logger.warning(f"Failed to parse {field} time '{time_str}': {e}")
                    continue
        
        # Fallback to current time if no valid alert time found
        logger.warning(f"No valid timestamp found in alert, using current time")
        return datetime.utcnow()
    
    def _flatten_dict(self, data, parent_key='', sep='_'):
        """
        Flatten a nested dictionary.
        
        Args:
            data: Dictionary to flatten
            parent_key: Key prefix for nested items
            sep: Separator between keys
        
        Returns:
            Flattened dictionary
        """
        items = []
        if isinstance(data, dict):
            for k, v in data.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                if isinstance(v, dict):
                    items.extend(self._flatten_dict(v, new_key, sep=sep).items())
                elif isinstance(v, list):
                    # For lists, we'll convert them to comma-separated strings or handle specially
                    if v and all(isinstance(x, (str, int, float)) for x in v):
                        items.append((new_key, ', '.join(str(x) for x in v)))
                    else:
                        # For complex lists, store as JSON string
                        items.append((new_key, json.dumps(v)))
                else:
                    items.append((new_key, v))
        else:
            items.append((parent_key, data))
        return dict(items)
    
    def _check_if_exists(self, key):
        """Check if an object already exists in S3."""
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                logger.error(f"Error checking if object exists: {e}")
                raise
    
    def _upload_to_s3(self, key, content):
        """Upload content to S3."""
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=content,
                ContentType='application/json'
            )
            return True
        except ClientError as e:
            logger.error(f"Error uploading to S3: {e}")
            return False
    
    def download_and_save_alerts(self, limit=None, save_batch=False):
        """
        Main method to download alerts and save to S3.
        
        Args:
            limit: Maximum number of alerts to process (None for all)
            save_batch: If True and format is ndjson, save all alerts in a single file
        """
        download_time = datetime.utcnow()
        
        logger.info(f"Fetching weather alerts at {download_time.isoformat()}Z...")
        
        try:
            # Get alerts from weather.gov
            alerts_data = self._get_alerts()
            
            if 'features' not in alerts_data:
                logger.warning("No 'features' field found in the response.")
                return
            
            alerts = alerts_data['features']
            logger.info(f"Found {len(alerts)} total alerts.")
            
            # Apply limit if specified
            if limit:
                alerts = alerts[:limit]
                logger.info(f"Processing first {limit} alerts.")
            
            # For batch NDJSON processing
            batch_data = []
            
            # Process each alert
            saved_count = 0
            skipped_count = 0
            error_count = 0
            
            for i, alert in enumerate(alerts, 1):
                try:
                    # Extract alert properties
                    properties = alert.get('properties', {})
                    
                    # Extract alert ID (use the ID field from properties or generate one)
                    alert_id = properties.get('id', '')
                    if not alert_id:
                        # Generate a deterministic ID based on alert content
                        alert_str = json.dumps(alert, sort_keys=True)
                        alert_id = hashlib.sha256(alert_str.encode()).hexdigest()[:16]
                    
                    # Parse alert time for partitioning
                    alert_time = self._parse_alert_time(alert)
                    
                    # Prepare flat structure with metadata
                    flat_alert = {
                        'download_timestamp': download_time.isoformat() + 'Z',
                        'alert_id': alert_id,
                        'alert_year': alert_time.strftime('%Y'),
                        'alert_month': alert_time.strftime('%m'),
                        'alert_day': alert_time.strftime('%d'),
                        'alert_hour': alert_time.strftime('%H')
                    }
                    
                    # Flatten the entire alert structure
                    flattened_data = self._flatten_dict(alert)
                    
                    # Merge with metadata
                    flat_alert.update(flattened_data)
                    
                    if save_batch and self.output_format == 'ndjson':
                        # Add to batch for later processing
                        batch_data.append(flat_alert)
                        saved_count += 1
                    else:
                        # Save individual files
                        # Generate S3 key using alert time
                        s3_key = self._generate_s3_key(alert_id, alert_time)
                        
                        # Check if already exists (idempotency)
                        if self._check_if_exists(s3_key):
                            logger.info(f"[{i}/{len(alerts)}] Alert {alert_id} already exists at {s3_key}, skipping...")
                            skipped_count += 1
                            continue
                        
                        # Convert to JSON
                        if self.output_format == 'ndjson':
                            alert_json = json.dumps(flat_alert, separators=(',', ':'))  # Compact format for NDJSON
                        else:
                            alert_json = json.dumps(flat_alert, indent=2)
                        
                        # Upload to S3
                        event_type = properties.get('event', 'Unknown')
                        severity = properties.get('severity', 'Unknown')
                        logger.info(f"[{i}/{len(alerts)}] Uploading alert {alert_id} ({event_type}, {severity}) to {s3_key}...")
                        
                        if self._upload_to_s3(s3_key, alert_json):
                            saved_count += 1
                            logger.info(f"[{i}/{len(alerts)}] Successfully uploaded alert {alert_id}")
                        else:
                            error_count += 1
                            logger.error(f"[{i}/{len(alerts)}] Failed to upload alert {alert_id}")
                
                except Exception as e:
                    error_count += 1
                    logger.error(f"[{i}/{len(alerts)}] Error processing alert: {e}")
                    continue
            
            # Handle batch upload if requested
            if save_batch and self.output_format == 'ndjson' and batch_data:
                # For batch files, we'll use download time since it contains mixed alert times
                batch_filename = f"alerts_batch_{download_time.strftime('%Y%m%d_%H%M%S')}.ndjson"
                year = download_time.strftime('%Y')
                month = download_time.strftime('%m')
                day = download_time.strftime('%d')
                hour = download_time.strftime('%H')
                batch_key = f"weather-alerts/batch/{year}/{month}/{day}/{hour}/{batch_filename}"
                
                # Convert to NDJSON
                ndjson_content = '\n'.join(json.dumps(alert, separators=(',', ':')) for alert in batch_data)
                
                logger.info(f"Uploading batch file with {len(batch_data)} alerts to {batch_key}...")
                if self._upload_to_s3(batch_key, ndjson_content):
                    logger.info(f"Successfully uploaded batch file")
                else:
                    error_count += len(batch_data)
                    saved_count = 0
                    logger.error(f"Failed to upload batch file")
            
            logger.info(f"\nSummary: Saved {saved_count} new alerts, skipped {skipped_count} existing alerts, {error_count} errors.")
            
            return {
                'saved': saved_count,
                'skipped': skipped_count,
                'errors': error_count,
                'total': len(alerts)
            }
            
        except Exception as e:
            logger.error(f"Fatal error in download_and_save_alerts: {e}")
            raise


def main():
    """Example usage of the WeatherAlertsDownloader."""
    # Configuration
    BUCKET_NAME = "johndauphine-weather-gov"  # Replace with your actual bucket name
    
    # Create downloader instance with different output formats
    
    # Option 1: Pretty-printed JSON (default)
    downloader = WeatherAlertsDownloader(bucket_name=BUCKET_NAME)
    
    # Option 2: Newline-delimited JSON (better for analytics)
    # downloader = WeatherAlertsDownloader(bucket_name=BUCKET_NAME, output_format='ndjson')
    
    # Option 3: Use a specific AWS profile
    # downloader = WeatherAlertsDownloader(
    #     bucket_name=BUCKET_NAME, 
    #     aws_profile="your-profile",
    #     output_format='ndjson'
    # )
    
    # Download and save alerts
    try:
        # Save individual files (default)
        results = downloader.download_and_save_alerts()
        
        # Or save as a single batch file (only for ndjson format)
        # results = downloader.download_and_save_alerts(save_batch=True)
        
        logger.info(f"Process completed successfully: {results}")
    except Exception as e:
        logger.error(f"Process failed: {e}")
        exit(1)


# AWS Glue Job Entry Point
def glue_job_main(args):
    """
    Entry point for AWS Glue job.
    
    Args:
        args: Dictionary of job arguments passed from Glue
    """
    # Get parameters from Glue job
    bucket_name = args.get('bucket_name', args.get('--bucket_name'))
    output_format = args.get('output_format', args.get('--output_format', 'json'))
    save_batch = args.get('save_batch', args.get('--save_batch', 'false')).lower() == 'true'
    
    if not bucket_name:
        raise ValueError("bucket_name parameter is required")
    
    logger.info(f"Starting Glue job with bucket: {bucket_name}, format: {output_format}")
    
    # Create downloader and run
    downloader = WeatherAlertsDownloader(
        bucket_name=bucket_name,
        output_format=output_format
    )
    
    results = downloader.download_and_save_alerts(save_batch=save_batch)
    logger.info(f"Glue job completed: {results}")
    
    return results


if __name__ == "__main__":
    # For local testing
    main()
    
    # For AWS Glue, use this instead:
    # from awsglue.utils import getResolvedOptions
    # import sys
    # args = getResolvedOptions(sys.argv, ['bucket_name', 'output_format', 'save_batch'])
    # glue_job_main(args)