"""
GCP Log Forwarder for ALB and other GCP logs

Pulls GCP Pub/Sub messages containing log entries and forwards them to CubeAPM.
"""

import gzip
import json
import logging
import os
import signal
import sys
import threading
import time
import urllib.request
from typing import Dict, List, Any

from google.cloud import pubsub_v1
from flask import Flask, jsonify

# Configure logging
logging.basicConfig(
  level=logging.INFO,
  format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
  stream=sys.stdout,
  force=True
)

logger = logging.getLogger(__name__)

# Configuration
LOG_ENDPOINT = os.environ.get("LOG_ENDPOINT")
if not LOG_ENDPOINT:
  raise ValueError("LOG_ENDPOINT environment variable is required")

# Pub/Sub Configuration
SUBSCRIPTION_NAME = os.environ.get("PUBSUB_SUBSCRIPTION")
if not SUBSCRIPTION_NAME:
  raise ValueError("PUBSUB_SUBSCRIPTION environment variable is required")

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
start_time = time.time()


def is_alb_log(log_entry: Dict[str, Any]) -> bool:
  """Check if log entry is an ALB log"""
  if "httpRequest" in log_entry and "resource" in log_entry:
    resource_type = log_entry.get("resource", {}).get("type", "")
    if resource_type == "http_load_balancer":
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


def signal_handler(signum):
    """Handle shutdown signals gracefully"""
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_event.set()


app = Flask(__name__)

@app.route("/health")
def health():
  return jsonify({
    'status': 'healthy',
    'uptime': time.time() - start_time,
  }), 200

def start_health_server():
  """Start Flask server for health checks"""
  port = int(os.environ.get("PORT", "8080"))
  logger.info(f"Health check server started on port {port}")
  
  # Run Flask server in a separate thread
  def run_server():
    try:
      app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)
    except Exception as e:
      logger.error(f"Health server error: {e}")
  health_thread = threading.Thread(target=run_server, daemon=True)
  health_thread.start()

def ship_logs(log_entries: List[str]) -> bool:
  """Ship multiple log entries to CubeAPM endpoint"""
  if not log_entries:
    return True
  
  try:
    jsonlines_payload = '\n'.join(log_entries) + '\n'
    gzipped_data = gzip.compress(jsonlines_payload.encode('utf-8'))
    
    headers = {
      'Content-Type': 'application/x-ndjson',
      'Content-Encoding': 'gzip',
      'Cube-Stream-Fields': CUBE_STREAM_FIELDS,
      'Cube-Time-Field': 'timestamp'
    }
    if CUBE_EXTRA_FIELDS:
      headers['Cube-Extra-Fields'] = CUBE_EXTRA_FIELDS

    request = urllib.request.Request(LOG_ENDPOINT, data=gzipped_data, headers=headers, method='POST')
    with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT) as response:
      return 200 <= response.status < 300

  except Exception as e:
    logger.error(f"Failed to ship logs: {e}")
    return False


def main():
  try:
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
  except Exception as e:
    logger.error(f"Error setting signal handlers: {e}")

  logger.info("Starting GCP Log Forwarder...")
  logger.info(f"Log Endpoint: {LOG_ENDPOINT}")

  start_health_server()
  subscriber = pubsub_v1.SubscriberClient()    
  subscription_path = SUBSCRIPTION_NAME
    
  while not shutdown_event.is_set():
    try:
      logger.info("Pulling messages from Pub/Sub...")
      response = subscriber.pull(
        request={"subscription": subscription_path, "max_messages": MAX_MESSAGES}
      )
      if not response.received_messages:
        time.sleep(5)  # No messages, wait a bit
        continue
                
      all_logs = []
      message_results = []
      
      logger.info(f"Received {len(response.received_messages)} messages from Pub/Sub")
      for message in response.received_messages:
        message_data = message.message.data
        if not message_data:
          logger.info("Received empty message data")
          continue
        decoded_str = message_data.decode("utf-8")
        if not decoded_str:
          logger.info("Received empty decoded message")
          continue
        parsed_data = json.loads(decoded_str)
        log_entries = parsed_data if isinstance(parsed_data, list) else [parsed_data]
        for log_entry in log_entries:
          if is_alb_log(log_entry):
            processed_log = process_alb_logs(log_entry)
            all_logs.append(processed_log)
            message_results.append(message.ack_id)
          else:
            logger.info(f"Non-ALB log received: {log_entry}")
            message_results.append(message.ack_id)

      logger.info(f"Processed {len(all_logs)} logs")
      
      # step 2: ship logs
      logger.info(f"Shipping {len(all_logs)} logs to CubeAPM")
      if all_logs:
        shipping_success = ship_logs(all_logs)
        if shipping_success:
          logger.info(f"Successfully shipped {len(all_logs)} logs to CubeAPM")
          subscriber.acknowledge(request={"subscription": subscription_path, "ack_ids": message_results})
          time.sleep(0.5)
            
    except Exception as e:
      logger.error(f"Error in main loop: {e}")
      time.sleep(1)


if __name__ == "__main__":
  main()
