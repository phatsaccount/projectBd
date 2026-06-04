"""
Core Data Pipeline DAG

Runs the local MovieMaster batch pipeline:
raw CSV -> landing parquet -> validation report -> cleaned parquet -> feature parquet
-> quality report -> MinIO copy for demo inspection.
"""

import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import requests
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


WORKSPACE_DIR = os.getenv("WORKSPACE_DIR", "/workspace")
BATCH_DIR = f"{WORKSPACE_DIR}/infra/batch"
DATA_DIR = f"{WORKSPACE_DIR}/data"
PYTHON_BIN = os.getenv("PIPELINE_PYTHON", "python")

RAW_DIR = os.getenv("RAW_LOCAL_DIR", f"{DATA_DIR}/archive")
LANDING_DIR = os.getenv("LANDING_LOCAL_DIR", f"{DATA_DIR}/landing")
CLEANED_DIR = os.getenv("CLEANED_LOCAL_DIR", f"{DATA_DIR}/cleaned")
FEATURES_DIR = os.getenv("FEATURES_LOCAL_DIR", f"{DATA_DIR}/features")
VALIDATION_REPORT_PATH = os.getenv(
    "VALIDATION_REPORT_PATH", f"{LANDING_DIR}/_validation/report"
)
QUALITY_REPORT_PATH = os.getenv("QUALITY_REPORT_PATH", f"{FEATURES_DIR}/_quality/report")

PIPELINE_ENV = {
    "PYTHONUNBUFFERED": "1",
    "SPARK_LOCAL_HOSTNAME": os.getenv("SPARK_LOCAL_HOSTNAME", "localhost"),
    "SPARK_LOCAL_IP": os.getenv("SPARK_LOCAL_IP", "127.0.0.1"),
    "SPARK_SQL_SHUFFLE_PARTITIONS": os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "4"),
    "RAW_LOCAL_DIR": RAW_DIR,
    "LANDING_LOCAL_DIR": LANDING_DIR,
    "CLEANED_LOCAL_DIR": CLEANED_DIR,
    "FEATURES_LOCAL_DIR": FEATURES_DIR,
    "VALIDATION_REPORT_PATH": VALIDATION_REPORT_PATH,
    "QUALITY_REPORT_PATH": QUALITY_REPORT_PATH,
    "MINIO_ENDPOINT": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
    "MINIO_ACCESS_KEY": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    "MINIO_SECRET_KEY": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    "MINIO_BUCKET": os.getenv("MINIO_BUCKET", "moviemaster"),
    "MINIO_SECURE": os.getenv("MINIO_SECURE", "false"),
}

RAW_FILES = [
    "movie.csv",
    "rating.csv",
    "tag.csv",
    "link.csv",
    "genome_scores.csv",
    "genome_tags.csv",
]

default_args = {
    "owner": "data-engineering",
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False,
}


def pre_flight_check():
    missing_scripts = [
        script
        for script in [
            "ingest_raw_data.py",
            "validate_raw_data.py",
            "clean_movie_data.py",
            "build_features.py",
            "quality_checks.py",
        ]
        if not Path(BATCH_DIR, script).exists()
    ]
    if missing_scripts:
        raise AirflowException(f"Missing batch scripts in {BATCH_DIR}: {missing_scripts}")

    if not shutil.which("java"):
        raise AirflowException("Java is not available in Airflow image; PySpark cannot run.")

    missing_raw = [name for name in RAW_FILES if not Path(RAW_DIR, name).exists()]
    if missing_raw:
        raise AirflowException(f"Missing raw CSV files in {RAW_DIR}: {missing_raw}")

    for path in [LANDING_DIR, CLEANED_DIR, FEATURES_DIR]:
        Path(path).mkdir(parents=True, exist_ok=True)

    try:
        response = requests.get(
            f"{PIPELINE_ENV['MINIO_ENDPOINT']}/minio/health/live", timeout=10
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        raise AirflowException(f"MinIO is not reachable: {exc}") from exc

    print("Pre-flight checks passed")
    print(f"Raw data: {RAW_DIR}")
    print(f"Landing output: {LANDING_DIR}")
    print(f"Cleaned output: {CLEANED_DIR}")
    print(f"Feature output: {FEATURES_DIR}")


def post_pipeline_notification():
    print("Core data pipeline completed successfully")
    print(f"Validation report: {VALIDATION_REPORT_PATH}")
    print(f"Quality report: {QUALITY_REPORT_PATH}")
    print(f"MinIO bucket: {PIPELINE_ENV['MINIO_BUCKET']}")


def batch_task(task_id, script_name, doc):
    return BashOperator(
        task_id=task_id,
        bash_command=(
            "set -euo pipefail\n"
            f"cd {WORKSPACE_DIR}\n"
            f"{PYTHON_BIN} -u {BATCH_DIR}/{script_name}"
        ),
        env=PIPELINE_ENV,
        append_env=True,
        execution_timeout=timedelta(hours=2),
        doc=doc,
    )


with DAG(
    "core_data_pipeline",
    default_args=default_args,
    description="Batch ETL: ingest, validate, clean, build features, quality check, upload to MinIO",
    schedule_interval="0 1 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["data-engineering", "batch-etl", "demo"],
    max_active_runs=1,
    doc_md=__doc__,
) as dag:
    pre_flight = PythonOperator(
        task_id="pre_flight_check",
        python_callable=pre_flight_check,
        doc="Validate Airflow image, repo mount, raw files, and MinIO availability.",
    )

    with TaskGroup("data_pipeline", tooltip="Core ETL Processing") as tg_pipeline:
        task_ingest = batch_task(
            "ingest_raw_data",
            "ingest_raw_data.py",
            "Ingest raw MovieLens CSV files into landing parquet.",
        )
        task_validate = batch_task(
            "validate_raw_data",
            "validate_raw_data.py",
            "Validate landing data and write a JSON report.",
        )
        task_clean = batch_task(
            "clean_movie_data",
            "clean_movie_data.py",
            "Clean and standardize landing parquet data.",
        )
        task_build_features = batch_task(
            "build_features",
            "build_features.py",
            "Build movie feature datasets for search and ML.",
        )
        task_quality_check = batch_task(
            "quality_checks",
            "quality_checks.py",
            "Generate cleaned/features data quality report.",
        )

        task_ingest >> task_validate >> task_clean >> task_build_features >> task_quality_check

    upload_to_minio = BashOperator(
        task_id="upload_outputs_to_minio",
        bash_command=(
            "set -euo pipefail\n"
            f"cd {WORKSPACE_DIR}\n"
            f"{PYTHON_BIN} -u infra/scripts/upload_directory_to_minio.py "
            f"--local-path {LANDING_DIR} --prefix landing --clear-prefix\n"
            f"{PYTHON_BIN} -u infra/scripts/upload_directory_to_minio.py "
            f"--local-path {CLEANED_DIR} --prefix cleaned --clear-prefix\n"
            f"{PYTHON_BIN} -u infra/scripts/upload_directory_to_minio.py "
            f"--local-path {FEATURES_DIR} --prefix features --clear-prefix"
        ),
        env=PIPELINE_ENV,
        append_env=True,
        execution_timeout=timedelta(minutes=30),
        doc="Copy local pipeline outputs to MinIO so they are visible in the MinIO console.",
    )

    post_notification = PythonOperator(
        task_id="post_pipeline_notification",
        python_callable=post_pipeline_notification,
        doc="Log final output locations.",
    )

    pre_flight >> tg_pipeline >> upload_to_minio >> post_notification
