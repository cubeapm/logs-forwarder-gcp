FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py /app/main.py
ENV PYTHONUNBUFFERED=1
ENV PORT=8080

# Create directory for gcloud credentials (for local development)
RUN mkdir -p /root/.config/gcloud

# For local development, mount your gcloud credentials (includes application_default_credentials.json):
# docker run -v ~/.config/gcloud:/root/.config/gcloud -e GOOGLE_CLOUD_PROJECT=able-reef-466806-u4 ...
# Or use a service account key:
# docker run -v /path/to/key.json:/tmp/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/tmp/key.json ...

CMD ["python","/app/main.py"]
