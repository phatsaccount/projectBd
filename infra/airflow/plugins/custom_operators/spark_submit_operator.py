"""
Custom Spark submit operator for ProjectBd
"""

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import logging

logger = logging.getLogger(__name__)


class ProjectBdSparkSubmitOperator(SparkSubmitOperator):
    """
    Extended SparkSubmitOperator with ProjectBd-specific defaults
    """
    
    def __init__(self, *args, **kwargs):
        # Set ProjectBd-specific defaults
        if 'spark_binary' not in kwargs:
            kwargs['spark_binary'] = '/usr/local/spark/bin/spark-submit'
        
        if 'verbose' not in kwargs:
            kwargs['verbose'] = True
        
        super().__init__(*args, **kwargs)
        logger.info(f"ProjectBd Spark Submit Operator initialized for {kwargs.get('application', 'unknown')}")
