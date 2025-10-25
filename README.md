# GCP Cloud Function Log Forwarder

Processes GCP logs from Pub/Sub messages and forwards them to CubeAPM.

## Features

- **ALB Log Processing**: HTTP Load Balancer access logs
- **Generic Log Support**: Any GCP log format (flattened for processing)
- **Retry Logic**: Exponential backoff for external API calls
- **Data Compression**: gzip compression before transmission
- **Environment Metadata**: Optional environment field support

## Environment Variables

**Required:**
- `LOG_ENDPOINT` - Log ingestion endpoint URL

**Optional:**
- `CUBE_ENVIRONMENT_KEY` - Environment identifier key (default: cube.environment)
- `CUBE_ENVIRONMENT` - Environment identifier (e.g., "production", "staging")
- `CUBE_EXTRA_FIELDS` - Extra fields to add (format: key1=value1,key2=value2)
- `MAX_RETRIES` - Retry attempts (default: 3)
- `RETRY_BASE_DELAY` - Base delay for retry backoff in seconds (default: 1.0)
- `REQUEST_TIMEOUT` - HTTP timeout in seconds (default: 60)
- `CUBE_STREAM_FIELDS` - Stream fields for CubeAPM (default: "event.domain")

## Deployment

1. **Set environment variables:**
```bash
export LOG_ENDPOINT="https://your-cubeapm-endpoint.com/logs"
export PUBSUB_TOPIC="gcp-logs"
export CUBE_ENVIRONMENT="production"
```

2. **Deploy the Cloud Function:**
```bash
gcloud functions deploy logs-forwarder \
  --runtime python39 \
  --trigger-topic $PUBSUB_TOPIC \
  --entry-point cloud_function_handler \
  --set-env-vars LOG_ENDPOINT=$LOG_ENDPOINT \
  --set-env-vars CUBE_ENVIRONMENT=$CUBE_ENVIRONMENT
```

3. **Set up Pub/Sub topic:**
```bash
gcloud pubsub topics create gcp-logs
```

4. **Configure GCP log routing** to send logs to the Pub/Sub topic.

## Testing

Test locally with sample data:
```bash
LOG_ENDPOINT="https://test.example.com" python main.py
```

## Log Processing

- **Input**: GCP log entries from Pub/Sub (base64 encoded)
- **Output**: Flattened JSON with `event.domain` field
- **Transmission**: JSON Lines format, gzip compressed
