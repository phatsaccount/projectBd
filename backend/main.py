from fastapi import FastAPI
from backend.app.interfaces.api.routes import search, events, recommendations
from backend.app.interfaces.api.routes.events import initialize_producer
from backend.app.interfaces.api.routes.recommendations import initialize_recommendation_api
from backend.app.infrastructure.redis.client import initialize_redis
from backend.app.infrastructure.redis.recommendation_cache import initialize_recommendation_cache
import logging
import os
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Movie Recommendation API")


def _env_int(name: str, default: int) -> int:
	value = os.getenv(name)
	if not value:
		return default
	try:
		return int(value)
	except ValueError:
		logger.warning("Invalid integer for %s=%r; using %s", name, value, default)
		return default


@app.on_event("startup")
def startup_event():
	"""Initialize producers and connections on startup."""
	try:
		# Initialize event producer
		kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
		initialize_producer(bootstrap_servers=kafka_bootstrap_servers)
		logger.info("Event producer initialized: %s", kafka_bootstrap_servers)
	except Exception as e:
		logger.warning(f"Event producer initialization failed: {e}")
	
	try:
		# Initialize Redis client and recommendation cache
		redis_host = os.getenv("REDIS_HOST", "localhost")
		redis_port = _env_int("REDIS_PORT", 6379)
		redis_client = initialize_redis(host=redis_host, port=redis_port, db=0)
		cache = initialize_recommendation_cache(redis_client, ttl=3600)
		
		# Initialize recommendation API
		artifacts_dir_env = os.getenv("PHASE04_ARTIFACTS_DIR", "").strip()
		artifacts_dir = Path(artifacts_dir_env) if artifacts_dir_env else None
		initialize_recommendation_api(cache=cache, artifacts_dir=artifacts_dir)
		logger.info("Redis and recommendation cache initialized: %s:%s", redis_host, redis_port)
	except Exception as e:
		logger.warning(f"Redis initialization failed: {e}")


@app.get("/health")
def health() -> dict:
	return {"status": "ok"}


app.include_router(search.router)
app.include_router(events.router)
app.include_router(recommendations.router)
