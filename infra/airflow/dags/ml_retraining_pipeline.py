"""
ML Retraining Pipeline DAG.

Demo flow:
feature parquet + Phase 4 artifacts -> serving artifact validation -> evaluation summary
-> MinIO model registry metadata -> backend serving smoke check.
"""

import csv
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

import boto3
import requests
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator


WORKSPACE_DIR = os.getenv("WORKSPACE_DIR", "/workspace")
FEATURES_DIR = Path(os.getenv("FEATURES_LOCAL_DIR", f"{WORKSPACE_DIR}/data/features"))
ARTIFACTS_DIR = Path(
    os.getenv("PHASE04_ARTIFACTS_DIR", f"{WORKSPACE_DIR}/data/models/phase04")
)
API_BASE_URL = os.getenv("API_BASE_URL", "http://api:8000")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "moviemaster")

REQUIRED_ARTIFACTS = [
    "popularity.csv",
    "item_neighbors.jsonl",
    "user_neighbors.jsonl",
    "content_vectors.npy",
    "content_movie_ids.json",
    "evaluation.json",
]


default_args = {
    "owner": "ml-team",
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


def _count_minio_objects(prefix):
    client = _s3_client()
    paginator = client.get_paginator("list_objects_v2")
    return sum(
        len(page.get("Contents", []))
        for page in paginator.paginate(Bucket=MINIO_BUCKET, Prefix=prefix)
    )


def fetch_latest_features(**context):
    feature_sets = {}
    for name in ["movie_features", "ratings_stats", "tag_stats"]:
        path = FEATURES_DIR / name
        parquet_files = list(path.rglob("*.parquet")) if path.exists() else []
        feature_sets[name] = {
            "local_path": str(path),
            "parquet_files": len(parquet_files),
        }

    if not any(item["parquet_files"] for item in feature_sets.values()):
        raise AirflowException(f"No feature parquet files found under {FEATURES_DIR}")

    payload = {
        "features_dir": str(FEATURES_DIR),
        "feature_sets": feature_sets,
        "minio_features_objects": _count_minio_objects("features/"),
    }
    context["task_instance"].xcom_push(key="features_summary", value=payload)
    print(payload)


def validate_serving_artifacts(**context):
    missing = [
        name for name in REQUIRED_ARTIFACTS if not (ARTIFACTS_DIR / name).exists()
    ]
    if missing:
        raise AirflowException(f"Missing Phase 4 artifacts: {missing}")

    popularity_rows = 0
    with (ARTIFACTS_DIR / "popularity.csv").open("r", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        popularity_rows = max(sum(1 for _ in reader) - 1, 0)

    item_neighbor_rows = sum(
        1 for _ in (ARTIFACTS_DIR / "item_neighbors.jsonl").open("r", encoding="utf-8")
    )
    user_neighbor_rows = sum(
        1 for _ in (ARTIFACTS_DIR / "user_neighbors.jsonl").open("r", encoding="utf-8")
    )

    summary = {
        "artifacts_dir": str(ARTIFACTS_DIR),
        "required_artifacts": REQUIRED_ARTIFACTS,
        "popularity_rows": popularity_rows,
        "item_neighbor_rows": item_neighbor_rows,
        "user_neighbor_rows": user_neighbor_rows,
        "content_vectors_bytes": (ARTIFACTS_DIR / "content_vectors.npy").stat().st_size,
    }
    context["task_instance"].xcom_push(key="artifact_summary", value=summary)
    print(summary)


def evaluate_current_model(**context):
    evaluation_path = ARTIFACTS_DIR / "evaluation.json"
    with evaluation_path.open("r", encoding="utf-8") as handle:
        evaluation = json.load(handle)

    context["task_instance"].xcom_push(key="evaluation", value=evaluation)
    print(evaluation)


def register_model_to_minio(**context):
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    model_version = f"phase4-serving-{timestamp}"
    payload = {
        "model_name": "movie_recommender",
        "model_version": model_version,
        "registered_at": datetime.utcnow().isoformat() + "Z",
        "features_summary": context["task_instance"].xcom_pull(
            task_ids="fetch_latest_features", key="features_summary"
        ),
        "artifact_summary": context["task_instance"].xcom_pull(
            task_ids="validate_serving_artifacts", key="artifact_summary"
        ),
        "evaluation": context["task_instance"].xcom_pull(
            task_ids="evaluate_current_model", key="evaluation"
        ),
    }

    _put_minio_json("model-registry/movie_recommender/latest.json", payload)
    _put_minio_json(
        f"model-registry/movie_recommender/versions/{model_version}.json", payload
    )
    context["task_instance"].xcom_push(key="model_version", value=model_version)


def backend_serving_smoke_check(**context):
    health = requests.get(f"{API_BASE_URL}/health", timeout=10)
    health.raise_for_status()

    recs = requests.get(f"{API_BASE_URL}/recommendations/1", params={"k": 5}, timeout=20)
    recs.raise_for_status()

    payload = {
        "api_health": health.json(),
        "sample_recommendations": recs.json(),
        "model_version": context["task_instance"].xcom_pull(
            task_ids="register_model_to_minio", key="model_version"
        ),
    }
    print(payload)


with DAG(
    "ml_retraining_pipeline",
    default_args=default_args,
    description="Validate current recommender artifacts and publish model registry metadata to MinIO",
    schedule_interval="0 2 * * 1",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ml-ops", "model-registry", "demo"],
    max_active_runs=1,
    doc_md=__doc__,
) as dag:
    fetch_features = PythonOperator(
        task_id="fetch_latest_features",
        python_callable=fetch_latest_features,
    )
    validate_artifacts = PythonOperator(
        task_id="validate_serving_artifacts",
        python_callable=validate_serving_artifacts,
    )
    evaluate_model = PythonOperator(
        task_id="evaluate_current_model",
        python_callable=evaluate_current_model,
    )
    register_model = PythonOperator(
        task_id="register_model_to_minio",
        python_callable=register_model_to_minio,
    )
    serving_check = PythonOperator(
        task_id="backend_serving_smoke_check",
        python_callable=backend_serving_smoke_check,
    )

    fetch_features >> validate_artifacts >> evaluate_model >> register_model >> serving_check
