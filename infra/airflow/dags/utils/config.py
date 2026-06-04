"""
Configuration utilities for Airflow DAGs
"""

import os
from airflow.models import Variable

# Airflow Variables
SPARK_JOBS_PATH = Variable.get("spark_jobs_path", default="/workspace/spark-jobs")
MINIO_ENDPOINT = Variable.get("minio_endpoint", default="minio:9000")
MINIO_BUCKET = Variable.get("minio_bucket", default="projectbd-data")
MINIO_ACCESS_KEY = Variable.get("minio_access_key", default="minioadmin")
MINIO_SECRET_KEY = Variable.get("minio_secret_key", default="minioadmin")

KAFKA_BROKER = Variable.get("kafka_broker", default="kafka:9092")
REDIS_HOST = Variable.get("redis_host", default="redis")
REDIS_PORT = int(Variable.get("redis_port", default="6379"))

ELASTICSEARCH_HOST = Variable.get("es_host", default="elasticsearch")
ELASTICSEARCH_PORT = int(Variable.get("es_port", default="9200"))

# Email Configuration
MAIL_FROM = Variable.get("mail_from", default="airflow@projectbd.com")
SMTP_HOST = Variable.get("smtp_host", default="smtp.gmail.com")
SMTP_PORT = int(Variable.get("smtp_port", default="587"))

# Slack Configuration
SLACK_WEBHOOK_URL = Variable.get("slack_webhook_url", default="")

# Default retry and timeout values
DEFAULT_RETRIES = 2
DEFAULT_RETRY_DELAY_MINUTES = 5
DEFAULT_TIMEOUT_HOURS = 8
