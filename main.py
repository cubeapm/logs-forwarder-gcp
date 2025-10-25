"""
GCP Log Forwarder for ALB and other GCP logs

Pulls GCP Pub/Sub messages containing log entries and forwards them to CubeAPM.
"""

import base64
import gzip
import json
import logging
import os
import signal
import socket
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Any

from google.cloud import pubsub_v1

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout,
    force=True
)
logger = logging.getLogger(__name__)

# Configuration
LOG_ENDPOINT = os.environ.get("LOG_ENDPOINT") or "https://4b4f796f2628.ngrok-free.app/api/logs/insert/jsonline"
if not LOG_ENDPOINT:
    raise ValueError("LOG_ENDPOINT environment variable is required")

# Pub/Sub Configuration
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT") or "able-reef-466806-u4"
SUBSCRIPTION_NAME = os.environ.get("PUBSUB_SUBSCRIPTION") or "projects/able-reef-466806-u4/subscriptions/pull-subs"
if not PROJECT_ID or not SUBSCRIPTION_NAME:
    raise ValueError("GOOGLE_CLOUD_PROJECT and PUBSUB_SUBSCRIPTION environment variables are required")

# CubeAPM Configuration
CUBE_ENVIRONMENT_KEY = os.environ.get("CUBE_ENVIRONMENT_KEY", "cube.environment")
CUBE_ENVIRONMENT = os.environ.get("CUBE_ENVIRONMENT")
CUBE_EXTRA_FIELDS = os.environ.get("CUBE_EXTRA_FIELDS", "")
CUBE_STREAM_FIELDS = os.environ.get("CUBE_STREAM_FIELDS", "event.domain")
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "60"))

# Processing Configuration
MAX_MESSAGES = int(os.environ.get("MAX_MESSAGES", "100"))
ACK_DEADLINE = int(os.environ.get("ACK_DEADLINE", "60"))
PROCESSING_TIMEOUT = int(os.environ.get("PROCESSING_TIMEOUT", "300"))

# Global variables for graceful shutdown
shutdown_event = threading.Event()
processed_count = 0
error_count = 0


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


def process_pubsub_message(message_data: bytes, message_id: str) -> bool:
    """Process a single Pub/Sub message containing log entries"""
    global processed_count, error_count
    
    try:
        # Decode the message data
        decoded_str = message_data.decode("utf-8")
        logger.info(f"Processing Pub/Sub message (message_id: {message_id})")
        
        # Parse the decoded message - could be a single log entry or an array of entries
        parsed_data = json.loads(decoded_str)
        
        # Handle both single log entry and array of log entries
        if isinstance(parsed_data, list):
            log_entries = parsed_data
            logger.info(f"Processing {len(log_entries)} log entries (message_id: {message_id})")
        else:
            log_entries = [parsed_data]
            logger.info(f"Processing single log entry (message_id: {message_id})")
        
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
            success = ship_logs(processed_logs, f"pubsub_message_{message_id}")
            
            if success:
                logger.info(f"Successfully processed {alb_count} ALB log entries (message_id: {message_id})")
                processed_count += alb_count
                return True
            else:
                logger.error(f"Failed to ship ALB logs (message_id: {message_id})")
                error_count += 1
                return False
        else:
            logger.info(f"No ALB logs found in message (message_id: {message_id})")
            return True
            
    except Exception as e:
        logger.error(f"Error processing Pub/Sub message (message_id: {message_id}): {e}")
        error_count += 1
        return False


def message_callback(message):
    """Callback function for Pub/Sub messages"""
    try:
        message_id = message.message_id or 'unknown'
        logger.info(f"Received message (message_id: {message_id})")
        
        # Process the message
        success = process_pubsub_message(message.data, message_id)
        
        if success:
            # Acknowledge the message
            message.ack()
            logger.info(f"Message acknowledged (message_id: {message_id})")
        else:
            # Nack the message to retry later
            message.nack()
            logger.warning(f"Message nacked for retry (message_id: {message_id})")
            
    except Exception as e:
        logger.error(f"Error in message callback: {e}")
        message.nack()


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_event.set()


def start_health_server():
    """Start a simple HTTP server for health checks"""
    from http.server import HTTPServer, BaseHTTPRequestHandler
    
    class HealthHandler(BaseHTTPRequestHandler):
      def do_GET(self):
          if self.path == '/':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {
                'status': 'healthy',
                'processed_count': processed_count,
                'error_count': error_count,
                'uptime': time.time() - start_time
            }
            self.wfile.write(json.dumps(response).encode())

    port = int(os.environ.get("PORT", "8080"))
    server = HTTPServer(('0.0.0.0', port), HealthHandler)
    logger.info(f"Health check server started on port {port}")
    return server

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

def main():
    global start_time
    start_time = time.time()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info("Starting GCP Log Forwarder...")
    logger.info(f"Project ID: {PROJECT_ID}")
    logger.info(f"Subscription: {SUBSCRIPTION_NAME}")
    logger.info(f"Log Endpoint: {LOG_ENDPOINT}")

    server = start_health_server()

    threading.Thread(
        target=server.serve_forever,
        daemon=True
    ).start()

    subscriber = pubsub_v1.SubscriberClient()
    # subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)
    subscription_path = SUBSCRIPTION_NAME

    flow_control = pubsub_v1.types.FlowControl(max_messages=MAX_MESSAGES)

    streaming_pull_future = subscriber.subscribe(
        subscription_path,
        callback=message_callback,
        flow_control=flow_control,
    )
    logger.info("Streaming subscription started")

    try:
        streaming_pull_future.result()
    except Exception as e:
        logger.error(f"Streaming pull stopped: {e}")

