"""
GCP Cloud Function Log Forwarder for ALB and other GCP logs

Processes GCP Pub/Sub messages containing log entries and forwards them to CubeAPM.
"""

import base64
import gzip
import json
import logging
import os
import socket
import sys
import urllib.error
import urllib.parse
import urllib.request
import functions_framework
from typing import Dict, List, Any

# Configure logging for GCP Cloud Functions
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout,
    force=True  # Force reconfiguration
)
logger = logging.getLogger(__name__)

# Configuration
LOG_ENDPOINT = os.environ.get("LOG_ENDPOINT") or "https://4b4f796f2628.ngrok-free.app/api/logs/insert/jsonline"
if not LOG_ENDPOINT:
  raise ValueError("LOG_ENDPOINT environment variable is required")

CUBE_ENVIRONMENT_KEY = os.environ.get("CUBE_ENVIRONMENT_KEY", "cube.environment")
CUBE_ENVIRONMENT = os.environ.get("CUBE_ENVIRONMENT")
CUBE_EXTRA_FIELDS = os.environ.get("CUBE_EXTRA_FIELDS", "")
CUBE_STREAM_FIELDS = os.environ.get("CUBE_STREAM_FIELDS", "event.domain")
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "60"))


def is_alb_log(log_entry: Dict[str, Any]) -> bool:
  """Check if log entry is an ALB log"""
  if "httpRequest" in log_entry and "resource" in log_entry:
    resource_type = log_entry.get("resource", {}).get("type", "")
    if resource_type == "http_load_balancer":
      return True

  log_name = log_entry.get("logName", "").lower()
  if "requests" in log_name:
    return True

  return False


def flatten_dict(obj: Dict[str, Any], parent_key: str = '', sep: str = '.') -> Dict[str, Any]:
  """Flatten a nested dictionary into dot-notation keys"""
  result = {}
  for key, value in obj.items():
    new_key = f"{parent_key}{sep}{key}" if parent_key else key
    if isinstance(value, dict):
      result.update(flatten_dict(value, new_key, sep=sep))
    elif isinstance(value, list):
      if len(value) == 0:
        result[new_key] = ''
      elif isinstance(value[0], dict):
        for i, item in enumerate(value):
          if isinstance(item, dict):
            result.update(flatten_dict(item, f"{new_key}.{i}", sep=sep))
          else:
            result[f"{new_key}.{i}"] = item
      else:
        result[new_key] = ','.join(str(v) for v in value) if value else ''
    else:
      result[new_key] = value
  return result


def process_alb_logs(log_entry: Dict[str, Any]) -> str:
  """Process ALB/HTTP Load Balancer logs - returns JSON string"""
  flattened_log = flatten_dict(log_entry)
  flattened_log["event.domain"] = "gcp.alb"
  if CUBE_ENVIRONMENT:
    flattened_log[CUBE_ENVIRONMENT_KEY] = CUBE_ENVIRONMENT
  return json.dumps(flattened_log)




def ship_logs(log_entries: List[str], source: str) -> bool:
  """Ship multiple log entries to CubeAPM endpoint"""
  if not log_entries:
    logger.info("No log entries to ship")
    return True
  
  try:
    # Join all log entries with newlines to create JSONL format
    jsonlines_payload = '\n'.join(log_entries) + '\n'
    gzipped_data = gzip.compress(jsonlines_payload.encode('utf-8'))
    
    headers = {
      'Content-Type': 'application/x-ndjson',
      'Content-Encoding': 'gzip',
      'Cube-Stream-Fields': CUBE_STREAM_FIELDS,
      'Cube-Time-Field': 'timestamp'
    }
    extra_fields = CUBE_EXTRA_FIELDS
    if extra_fields:
      headers['Cube-Extra-Fields'] = extra_fields  # key1=value1,key2=value2

    logger.info(f"Shipping {len(log_entries)} log entries (compressed size: {len(gzipped_data)} bytes) from {source}")
    return post_log(gzipped_data, headers, source)
    
  except Exception as e:
    logger.error(f"Failed to prepare logs for shipping: {e}. Source: {source}")
    return False


def post_log(payload: bytes, headers: Dict[str, str], source: str) -> bool:
  """Post data to CubeAPM endpoint - single attempt"""
  try:
    request = urllib.request.Request(
      LOG_ENDPOINT,
      data=payload,
      headers=headers,
      method='POST'
    )
    
    with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT) as response:
      status_code = response.status
      if 200 <= status_code < 300:
        logger.info(f"Successfully shipped logs (status: {status_code}) from {source}")
        return True
      else:
        response_text = response.read().decode('utf-8')
        logger.error(f"HTTP error {status_code} for {source}: {response_text}")
        return False
        
  except urllib.error.HTTPError as e:
    error_text = e.read().decode('utf-8') if hasattr(e, 'read') else str(e)
    logger.error(f"HTTP error {e.code} for {source}: {error_text}")
    return False
    
  except (urllib.error.URLError, socket.timeout, OSError) as e:
    logger.error(f"Network error for {source}: {e}")
    return False

@functions_framework.cloud_event
def cloud_function_handler(cloud_event):
  """Main Cloud Function handler for Pub/Sub messages - ALB logs only"""
  
  message_id = cloud_event.data["message"].get('messageId', 'unknown')
  decoded_str = None
  
  # Log function start
  logger.info(f"Cloud Function started processing message (message_id: {message_id})")
  # flush_logs()
  
  try:
    encoded_data = cloud_event.data["message"]["data"]
    decoded_bytes = base64.b64decode(encoded_data)
    decoded_str = decoded_bytes.decode("utf-8")
    
    logger.info(f"Processing log entries from Pub/Sub (message_id: {message_id})")
    # flush_logs()
    
    # Parse the decoded message - could be a single log entry or an array of entries
    parsed_data = json.loads(decoded_str)
    
    # Handle both single log entry and array of log entries
    if isinstance(parsed_data, list):
      log_entries = parsed_data
      logger.info(f"Processing {len(log_entries)} log entries (message_id: {message_id})")
    else:
      log_entries = [parsed_data]
      logger.info(f"Processing single log entry (message_id: {message_id})")
    
    # flush_logs()
    
    # Process each log entry
    processed_logs = []
    alb_count = 0
    skipped_count = 0
    
    for i, log_entry in enumerate(log_entries):
      if is_alb_log(log_entry):
        logger.info(f"Processing ALB log entry {i+1}/{len(log_entries)} (message_id: {message_id})")
        processed_log = process_alb_logs(log_entry)
        processed_logs.append(processed_log)
        alb_count += 1
      else:
        logger.info(f"Skipping unknown log entry {i+1}/{len(log_entries)} (message_id: {message_id})")
        skipped_count += 1
    
    # Ship all processed logs together
    if processed_logs:
      success = ship_logs(processed_logs, "pubsub_message_alb")
      
      if success:
        logger.info(f"Successfully processed {alb_count} ALB log entries (message_id: {message_id})")
        # flush_logs()
        return {
          'statusCode': 200,
          'body': {
            'message': 'ALB log processing complete',
            'message_id': message_id,
            'log_type': 'alb',
            'success': True,
            'processed_count': alb_count,
            'skipped_count': skipped_count,
            'total_count': len(log_entries),
            'error_message': None
          }
        }
      else:
        logger.error(f"Failed to ship ALB logs (message_id: {message_id})")
        return {
          'statusCode': 500,
          'body': {
            'message': 'ALB log shipping failed',
            'message_id': message_id,
            'log_type': 'alb',
            'success': False,
            'processed_count': alb_count,
            'skipped_count': skipped_count,
            'total_count': len(log_entries),
            'error_message': "Failed to ship logs"
          }
        }
    else:
      logger.info(f"No ALB logs found in message (message_id: {message_id})")
      return {
        'statusCode': 200,
        'body': {
          'message': 'No ALB logs found',
          'message_id': message_id,
          'log_type': 'unknown',
          'success': True,
          'processed_count': 0,
          'skipped_count': skipped_count,
          'total_count': len(log_entries),
          'error_message': 'No ALB logs to process'
        }
      }
      
  except Exception as e:
    if decoded_str:
      logger.error(f"Error processing Pub/Sub message (message_id: {message_id}): {e}")
      logger.error(f"Problematic message content: {decoded_str[:500]}{'...' if len(decoded_str) > 500 else ''}")
    else:
      logger.error(f"Error processing Pub/Sub message (message_id: {message_id}): {e}")
      logger.error(f"Failed to decode message data")
    
    return {
      'statusCode': 500,
      'body': {
        'message': 'Processing error',
        'message_id': message_id,
        'log_type': 'alb',
        'success': False,
        'processed_count': 0,
        'error_message': f"Processing error: {e}"
      }
    }