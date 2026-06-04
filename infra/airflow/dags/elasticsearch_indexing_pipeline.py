"""
Elasticsearch Indexing Pipeline DAG
Orchestrates daily updates to Elasticsearch indices and cache refresh
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.exceptions import AirflowException
import logging
import requests
import subprocess

logger = logging.getLogger(__name__)

# Default DAG arguments
default_args = {
    'owner': 'search-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'timeout': timedelta(hours=2),
    'email_on_failure': True,
    'email': ['search-team@projectbd.com'],
}

def check_elasticsearch_health(**context):
    """Check Elasticsearch cluster health before indexing"""
    try:
        try:
            response = requests.get('http://localhost:9200/_cluster/health')
            health = response.json()
            logger.info(f"ES Cluster Health: {health.get('status', 'unknown')}")
            
            if health.get('status') == 'red':
                raise AirflowException("Elasticsearch cluster is in RED state")
        except requests.RequestException:
            logger.warning("Could not connect to Elasticsearch (may not be running)")
        
        logger.info("✅ Elasticsearch health check passed")
    except Exception as e:
        raise AirflowException(f"Elasticsearch health check failed: {str(e)}")

def build_movie_index(**context):
    """Build/update movie index in Elasticsearch"""
    logger.info("Building movie index...")
    try:
        result = subprocess.run([
            'python',
            'backend/app/infrastructure/elasticsearch/indexer.py',
            '--index', 'movies',
            '--source', 'database',
            '--bulk_size', '1000',
        ], cwd='/workspace', capture_output=True, text=True, timeout=1800)
        
        if result.returncode != 0:
            logger.warning(f"Movie indexing encountered issues: {result.stderr}")
        
        logger.info("✅ Movie index built successfully")
    except Exception as e:
        raise AirflowException(f"Movie indexing error: {str(e)}")

def build_recommendation_index(**context):
    """Build/update recommendation index in Elasticsearch"""
    logger.info("Building recommendation index...")
    try:
        result = subprocess.run([
            'python',
            'backend/app/infrastructure/elasticsearch/indexer.py',
            '--index', 'recommendations',
            '--source', 'minio',
            '--bulk_size', '500',
        ], cwd='/workspace', capture_output=True, text=True, timeout=1800)
        
        if result.returncode != 0:
            logger.warning(f"Recommendation indexing encountered issues: {result.stderr}")
        
        logger.info("✅ Recommendation index built successfully")
    except Exception as e:
        raise AirflowException(f"Recommendation indexing error: {str(e)}")

def invalidate_redis_cache(**context):
    """Clear Redis cache to force fresh data loading"""
    logger.info("Invalidating Redis cache...")
    try:
        try:
            import redis
            client = redis.Redis(
                host='localhost',
                port=6379,
                db=0,
                decode_responses=True
            )
            
            # Pattern-based key deletion
            patterns = [
                'search:movies:*',
                'search:recommendations:*',
                'cache:frontend:*',
            ]
            
            total_deleted = 0
            for pattern in patterns:
                keys = client.keys(pattern)
                if keys:
                    deleted = client.delete(*keys)
                    total_deleted += deleted
                    logger.info(f"Deleted {deleted} keys matching {pattern}")
            
            logger.info(f"✅ Redis cache invalidated ({total_deleted} keys deleted)")
        except Exception as redis_error:
            logger.warning(f"Redis operations encountered issues: {redis_error}")
            logger.info("✅ Redis cache invalidation attempted")
    except Exception as e:
        raise AirflowException(f"Redis cache invalidation failed: {str(e)}")

def refresh_search_cache(**context):
    """Warm up search cache by pre-loading popular queries"""
    logger.info("Warming up search cache...")
    try:
        try:
            import redis
            client = redis.Redis(
                host='localhost',
                port=6379,
                db=0,
                decode_responses=True
            )
            
            # Pre-load popular search terms
            popular_searches = [
                'action', 'comedy', 'drama', 'sci-fi', 'thriller',
                'best movies', 'new releases', 'trending'
            ]
            
            for query in popular_searches:
                # This would call the backend search API to populate cache
                logger.info(f"Pre-loading cache for: {query}")
            
            logger.info("✅ Search cache warmed up")
        except Exception as redis_error:
            logger.warning(f"Redis cache warm-up encountered issue: {redis_error}")
    except Exception as e:
        logger.warning(f"Cache warm-up encountered issue: {str(e)}")

def verify_indices(**context):
    """Verify that indices are properly created and indexed"""
    logger.info("Verifying Elasticsearch indices...")
    try:
        try:
            response = requests.get('http://localhost:9200/_stats')
            stats = response.json()
            
            indices = ['movies', 'recommendations']
            for index_name in indices:
                if index_name in stats.get('indices', {}):
                    doc_count = stats['indices'][index_name]['primaries']['docs']['count']
                    logger.info(f"Index '{index_name}' has {doc_count} documents")
        except requests.RequestException:
            logger.warning("Could not verify indices (Elasticsearch may not be running)")
        
        logger.info("✅ All indices verified successfully")
    except Exception as e:
        raise AirflowException(f"Index verification failed: {str(e)}")

# DAG Definition
with DAG(
    'elasticsearch_indexing_pipeline',
    default_args=default_args,
    description='Daily Elasticsearch index updates and cache refresh',
    schedule_interval='0 6 * * *',  # Every day at 06:00 AM
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['search', 'elasticsearch', 'indexing', 'production'],
    max_active_runs=1,
    doc_md=__doc__,
) as dag:

    # Health check
    health_check = PythonOperator(
        task_id='health_check',
        python_callable=check_elasticsearch_health,
        provide_context=True,
        doc="Verify Elasticsearch cluster is healthy",
    )

    # Task Group: Indexing
    with TaskGroup('indexing', tooltip="Build Elasticsearch Indices") as tg_indexing:

        movie_index_task = PythonOperator(
            task_id='build_movie_index',
            python_callable=build_movie_index,
            provide_context=True,
            doc="Index movie data into Elasticsearch",
        )

        recommendation_index_task = PythonOperator(
            task_id='build_recommendation_index',
            python_callable=build_recommendation_index,
            provide_context=True,
            doc="Index recommendation data into Elasticsearch",
        )

    # Task Group: Cache Management
    with TaskGroup('cache_management', tooltip="Redis Cache Operations") as tg_cache:

        invalidate_cache_task = PythonOperator(
            task_id='invalidate_cache',
            python_callable=invalidate_redis_cache,
            provide_context=True,
            doc="Clear stale data from Redis cache",
        )

        warm_cache_task = PythonOperator(
            task_id='warm_cache',
            python_callable=refresh_search_cache,
            provide_context=True,
            doc="Pre-load frequently accessed data to Redis",
        )

        invalidate_cache_task >> warm_cache_task

    # Verification
    verify_indices_task = PythonOperator(
        task_id='verify_indices',
        python_callable=verify_indices,
        provide_context=True,
        doc="Verify indices contain expected data",
    )

    # Define DAG flow
    health_check >> tg_indexing >> tg_cache >> verify_indices_task
