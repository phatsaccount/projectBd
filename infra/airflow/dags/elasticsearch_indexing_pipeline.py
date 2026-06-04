"""
Elasticsearch Indexing Pipeline DAG.

Demo flow:
features/cleaned parquet -> Elasticsearch movies_v1 index -> Redis cache refresh
-> MinIO indexing report.
"""

import json
import os
import subprocess
from datetime import datetime, timedelta

import boto3
import redis
import requests
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator


WORKSPACE_DIR = os.getenv("WORKSPACE_DIR", "/workspace")
CLEANED_LOCAL_DIR = os.getenv("CLEANED_LOCAL_DIR", f"{WORKSPACE_DIR}/data/cleaned")
ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch:9200")
API_BASE_URL = os.getenv("API_BASE_URL", "http://api:8000")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "moviemaster")
MOVIES_INDEX = "movies_v1"


default_args = {
    "owner": "search-team",
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
    "depends_on_past": False,
}


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


def _ensure_bucket(client):
    buckets = client.list_buckets().get("Buckets", [])
    if not any(item.get("Name") == MINIO_BUCKET for item in buckets):
        client.create_bucket(Bucket=MINIO_BUCKET)


def _put_minio_json(key, payload):
    body = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
    client = _s3_client()
    _ensure_bucket(client)
    client.put_object(
        Bucket=MINIO_BUCKET,
        Key=key,
        Body=body,
        ContentType="application/json",
    )
    print(f"Wrote s3://{MINIO_BUCKET}/{key}")


def check_elasticsearch_health():
    response = requests.get(f"{ELASTICSEARCH_HOST}/_cluster/health", timeout=20)
    response.raise_for_status()
    health = response.json()
    print("Elasticsearch health:", health)
    if health.get("status") == "red":
        raise AirflowException("Elasticsearch cluster is RED")


def build_movie_index():
    env = os.environ.copy()
    env.update(
        {
            "ELASTICSEARCH_HOST": ELASTICSEARCH_HOST,
            "CLEANED_LOCAL_DIR": CLEANED_LOCAL_DIR,
            "FORCE_INDEX": "true",
            "SPARK_LOCAL_HOSTNAME": "localhost",
            "SPARK_LOCAL_IP": "127.0.0.1",
        }
    )

    result = subprocess.run(
        ["python", "-m", "backend.app.infrastructure.elasticsearch.indexer"],
        cwd=WORKSPACE_DIR,
        env=env,
        capture_output=True,
        text=True,
        timeout=1800,
    )
    print(result.stdout)
    if result.stderr:
        print(result.stderr)
    if result.returncode != 0:
        raise AirflowException(f"Movie indexing failed with code {result.returncode}")


def verify_movies_index(**context):
    count_response = requests.get(
        f"{ELASTICSEARCH_HOST}/{MOVIES_INDEX}/_count", timeout=20
    )
    count_response.raise_for_status()
    doc_count = int(count_response.json().get("count", 0))
    if doc_count <= 0:
        raise AirflowException(f"{MOVIES_INDEX} has no documents")

    search_response = requests.get(
        f"{API_BASE_URL}/movies/search",
        params={"q": "Toy", "page": 1, "size": 3},
        timeout=20,
    )
    search_response.raise_for_status()
    sample_hits = search_response.json().get("hits", [])

    report = {
        "index": MOVIES_INDEX,
        "doc_count": doc_count,
        "sample_query": "Toy",
        "sample_titles": [item.get("title") for item in sample_hits],
        "checked_at": datetime.utcnow().isoformat() + "Z",
    }
    context["task_instance"].xcom_push(key="index_report", value=report)
    print(report)


def invalidate_redis_cache():
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    patterns = ["search:movies:*", "search:recommendations:*", "cache:frontend:*"]
    total_deleted = 0
    for pattern in patterns:
        keys = client.keys(pattern)
        if keys:
            total_deleted += client.delete(*keys)
    print(f"Deleted {total_deleted} Redis cache keys")


def warm_search_cache():
    queries = ["Toy", "Matrix", "Comedy"]
    warmed = []
    for query in queries:
        response = requests.get(
            f"{API_BASE_URL}/movies/search",
            params={"q": query, "page": 1, "size": 3},
            timeout=20,
        )
        response.raise_for_status()
        warmed.append({"query": query, "total": response.json().get("total", 0)})
    print("Warmed search queries:", warmed)


def write_index_report(**context):
    report = context["task_instance"].xcom_pull(
        task_ids="verify_movies_index", key="index_report"
    )
    if not report:
        raise AirflowException("Missing index report from verify_movies_index")

    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    _put_minio_json(f"search-index/{MOVIES_INDEX}/latest.json", report)
    _put_minio_json(f"search-index/{MOVIES_INDEX}/runs/{timestamp}.json", report)


with DAG(
    "elasticsearch_indexing_pipeline",
    default_args=default_args,
    description="Build Elasticsearch movies_v1 index and write an indexing report to MinIO",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["search", "elasticsearch", "demo"],
    max_active_runs=1,
    doc_md=__doc__,
) as dag:
    health_check = PythonOperator(
        task_id="health_check",
        python_callable=check_elasticsearch_health,
    )
    build_index = PythonOperator(
        task_id="build_movie_index",
        python_callable=build_movie_index,
    )
    verify_index = PythonOperator(
        task_id="verify_movies_index",
        python_callable=verify_movies_index,
    )
    invalidate_cache = PythonOperator(
        task_id="invalidate_redis_cache",
        python_callable=invalidate_redis_cache,
    )
    warm_cache = PythonOperator(
        task_id="warm_search_cache",
        python_callable=warm_search_cache,
    )
    write_report = PythonOperator(
        task_id="write_index_report_to_minio",
        python_callable=write_index_report,
    )

    health_check >> build_index >> verify_index >> invalidate_cache >> warm_cache >> write_report
