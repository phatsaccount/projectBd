import logging

from airflow.exceptions import AirflowException

try:
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
except ModuleNotFoundError:
    SparkSubmitOperator = None

logger = logging.getLogger(__name__)


if SparkSubmitOperator is None:
    class ProjectBdSparkSubmitOperator:
        def __init__(self, *args, **kwargs):
            raise AirflowException(
                "apache-airflow-providers-apache-spark is not installed. "
                "Use the core_data_pipeline DAG's local PySpark tasks for this demo, "
                "or install the Spark provider before using ProjectBdSparkSubmitOperator."
            )
else:
    class ProjectBdSparkSubmitOperator(SparkSubmitOperator):
        """
        Extended SparkSubmitOperator with ProjectBd-specific defaults.
        """

        def __init__(self, *args, **kwargs):
            if "spark_binary" not in kwargs:
                kwargs["spark_binary"] = "/usr/local/spark/bin/spark-submit"

            if "verbose" not in kwargs:
                kwargs["verbose"] = True

            super().__init__(*args, **kwargs)
            logger.info(
                "ProjectBd Spark Submit Operator initialized for %s",
                kwargs.get("application", "unknown"),
            )
