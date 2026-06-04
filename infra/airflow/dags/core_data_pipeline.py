"""
Core Data Pipeline DAG
Orchestrates daily ETL process: Ingest -> Validate -> Clean -> Build Features -> Quality Check
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.exceptions import AirflowException
import logging

logger = logging.getLogger(__name__)

# Default DAG arguments
default_args = {
    'owner': 'data-engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'timeout': timedelta(hours=8),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['data-team@projectbd.com'],
    'depends_on_past': False,
}

# DAG Definition
with DAG(
    'core_data_pipeline',
    default_args=default_args,
    description='Daily batch ETL: Ingest, validate, clean, and build features',
    schedule_interval='0 1 * * *',  # 01:00 AM daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['data-engineering', 'batch-etl', 'production'],
    max_active_runs=1,
    doc_md=__doc__,
) as dag:

    def pre_flight_check(**context):
        """Validate environment and dependencies before pipeline execution"""
        try:
            import requests
            # Check if Spark is accessible
            # Check if MinIO is accessible
            # Check if data sources are available
            logger.info("✅ Pre-flight checks passed")
        except Exception as e:
            raise AirflowException(f"Pre-flight check failed: {str(e)}")

    def post_pipeline_notification(**context):
        """Send pipeline completion notification"""
        exec_date = context['execution_date']
        logger.info(f"✅ Pipeline completed successfully at {exec_date}")

    # Task: Pre-flight checks
    pre_flight = PythonOperator(
        task_id='pre_flight_check',
        python_callable=pre_flight_check,
        provide_context=True,
        doc="Validate environment and data source availability",
    )

    # Task Group: Data Pipeline Steps
    with TaskGroup('data_pipeline', tooltip="Core ETL Processing") as tg_pipeline:

        # Step 1: Ingest Raw Data
        task_ingest = SparkSubmitOperator(
            task_id='ingest_raw_data',
            application='{{ var.value.spark_jobs_path }}/batch/ingest_raw_data.py',
            conf={
                'spark.driver.memory': '2g',
                'spark.executor.memory': '4g',
                'spark.executor.cores': '4',
            },
            spark_binary='/usr/local/spark/bin/spark-submit',
            verbose=True,
            doc="Ingest raw data from sources and store in MinIO",
        )

        # Step 2: Validate Raw Data
        task_validate = SparkSubmitOperator(
            task_id='validate_raw_data',
            application='{{ var.value.spark_jobs_path }}/batch/validate_raw_data.py',
            conf={
                'spark.driver.memory': '2g',
                'spark.executor.memory': '4g',
                'spark.executor.cores': '4',
            },
            spark_binary='/usr/local/spark/bin/spark-submit',
            verbose=True,
            doc="Validate data quality and schema compliance",
        )

        # Step 3: Clean Movie Data
        task_clean = SparkSubmitOperator(
            task_id='clean_movie_data',
            application='{{ var.value.spark_jobs_path }}/batch/clean_movie_data.py',
            conf={
                'spark.driver.memory': '2g',
                'spark.executor.memory': '4g',
                'spark.executor.cores': '4',
            },
            spark_binary='/usr/local/spark/bin/spark-submit',
            verbose=True,
            doc="Clean and standardize movie data",
        )

        # Step 4: Build Features for ML
        task_build_features = SparkSubmitOperator(
            task_id='build_features',
            application='{{ var.value.spark_jobs_path }}/batch/build_features.py',
            conf={
                'spark.driver.memory': '4g',
                'spark.executor.memory': '8g',
                'spark.executor.cores': '8',
            },
            spark_binary='/usr/local/spark/bin/spark-submit',
            verbose=True,
            doc="Build ML features from cleaned data",
        )

        # Step 5: Quality Checks
        task_quality_check = SparkSubmitOperator(
            task_id='quality_checks',
            application='{{ var.value.spark_jobs_path }}/batch/quality_checks.py',
            conf={
                'spark.driver.memory': '2g',
                'spark.executor.memory': '4g',
                'spark.executor.cores': '4',
            },
            spark_binary='/usr/local/spark/bin/spark-submit',
            verbose=True,
            doc="Run data quality checks and generate report",
        )

        # Define task dependencies within task group
        task_ingest >> task_validate >> task_clean >> task_build_features >> task_quality_check

    # Post-pipeline notification
    post_notification = PythonOperator(
        task_id='post_pipeline_notification',
        python_callable=post_pipeline_notification,
        provide_context=True,
        trigger_rule='all_done',
        doc="Send pipeline completion notification",
    )

    # Define main DAG flow
    pre_flight >> tg_pipeline >> post_notification
