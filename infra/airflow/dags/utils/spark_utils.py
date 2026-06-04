"""
Spark utilities for Airflow DAGs
"""

import logging

logger = logging.getLogger(__name__)


def create_spark_config(driver_memory="2g", executor_memory="4g", executor_cores="4"):
    """
    Create standard Spark configuration dictionary
    
    Args:
        driver_memory: Spark driver memory (default: 2g)
        executor_memory: Spark executor memory (default: 4g)
        executor_cores: Number of executor cores (default: 4)
    
    Returns:
        Dictionary with Spark configuration
    """
    return {
        'spark.driver.memory': driver_memory,
        'spark.executor.memory': executor_memory,
        'spark.executor.cores': executor_cores,
        'spark.sql.shuffle.partitions': '200',
        'spark.default.parallelism': '200',
    }


def get_large_job_config():
    """Get Spark config for large ML jobs"""
    return create_spark_config(
        driver_memory="4g",
        executor_memory="8g",
        executor_cores="8"
    )


def get_standard_job_config():
    """Get Spark config for standard ETL jobs"""
    return create_spark_config(
        driver_memory="2g",
        executor_memory="4g",
        executor_cores="4"
    )


def get_small_job_config():
    """Get Spark config for small jobs"""
    return create_spark_config(
        driver_memory="1g",
        executor_memory="2g",
        executor_cores="2"
    )


logger.info("Spark utilities loaded")
