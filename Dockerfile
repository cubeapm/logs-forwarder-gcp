FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py /app/main.py
ENV PYTHONUNBUFFERED=1
ENV PORT=8080

CMD ["python","/app/main.py"]
