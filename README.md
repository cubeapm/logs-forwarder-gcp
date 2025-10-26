# GCP Log Forwarder for CubeAPM

Processes GCP logs from Pub/Sub messages and forwards them to CubeAPM.

## Features

- **ALB Log Processing**: HTTP Load Balancer access logs
- **Data Compression**: gzip compression before transmission


#### 3. Set Up Environment Variables

**Mandatory:**
- `LOG_ENDPOINT`: Your CubeAPM logs endpoint
  - Format: `http://<cubeapm-url>/api/logs/insert/jsonline`
  - Example: `http://your-instance.cubeapm.com/api/logs/insert/jsonline`
  - **Important**: Must include `/api/logs/insert/jsonline` path

- `PUBSUB_SUBSCRIPTION`: Full subscription path
  - Format: `projects/<PROJECT_ID>/subscriptions/<SUBSCRIPTION_NAME>`
  - Example: `projects/your-project-123/subscriptions/gcp-logs-sub`

**Optional:**
- `CUBE_ENVIRONMENT`: Environment identifier (e.g., "production", "staging")
- `CUBE_ENVIRONMENT_KEY`: Environment key field name (default: "cube.environment")
- `CUBE_EXTRA_FIELDS`: Additional fields (format: `key1=value1,key2=value2`)
- `CUBE_STREAM_FIELDS`: Stream fields for CubeAPM (default: "event.domain")
- `MAX_MESSAGES`: Max messages to pull per batch (default: 100)

## Deployment

### Run as Cloud Run Service

```bash
# Build and deploy
gcloud run deploy logs-forwarder \
  --source . \
  --set-env-vars LOG_ENDPOINT=http://your-instance.cubeapm.com/api/logs/insert/jsonline \
  --set-env-vars PUBSUB_SUBSCRIPTION=projects/$PROJECT_ID/subscriptions/$PUBSUB_SUB
```

## Local Development

### Prerequisites

1. **Install Python dependencies:**
```bash
pip install -r requirements.txt
```

2. **Set up GCP authentication:**
```bash
# Using application default credentials
gcloud auth application-default login

# Or download a service account key from GCP Console
# Save it to your local machine (e.g., ~/path/to/key.json)
export GOOGLE_APPLICATION_CREDENTIALS="~/path/to/key.json"
```

## Health Check

The forwarder includes a health check endpoint at `/health` on port 8080 that returns:
```json
{
  "status": "healthy",
  "uptime": 12345.67
}
```

## Monitoring

- Check logs in GCP Console under Cloud Function/Cloud Run logs
- Monitor health endpoint at `http://your-service-url/health:8080`
- Verify logs are being received in your CubeAPM dashboard
