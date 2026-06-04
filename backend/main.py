from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from backend.app.interfaces.api.routes import search, events, recommendations
from backend.app.interfaces.api.routes.events import initialize_producer
from backend.app.interfaces.api.routes.recommendations import initialize_recommendation_api
from backend.app.interfaces.api.metrics import PrometheusMiddleware, metrics_response
from backend.app.infrastructure.redis.client import initialize_redis
from backend.app.infrastructure.redis.recommendation_cache import initialize_recommendation_cache
import logging
import os
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Movie Recommendation API")
app.add_middleware(
	CORSMiddleware,
	allow_origins=[
		"http://localhost:5173",
		"http://127.0.0.1:5173",
	],
	allow_credentials=True,
	allow_methods=["*"],
	allow_headers=["*"],
)
app.add_middleware(PrometheusMiddleware)


@app.on_event("startup")
def startup_event():
	"""Initialize producers and connections on startup."""
	try:
		# Initialize event producer
		kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
		initialize_producer(bootstrap_servers=kafka_bootstrap_servers)
		logger.info("Event producer initialized")
	except Exception as e:
		logger.warning(f"Event producer initialization failed: {e}")
	
	try:
		# Initialize Redis client and recommendation cache
		redis_host = os.getenv("REDIS_HOST", "localhost")
		redis_port = int(os.getenv("REDIS_PORT", "6379"))
		redis_db = int(os.getenv("REDIS_DB", "0"))
		redis_client = initialize_redis(host=redis_host, port=redis_port, db=redis_db)
		cache = initialize_recommendation_cache(redis_client, ttl=3600)
		
		# Initialize recommendation API
		artifacts_dir = os.getenv("PHASE04_ARTIFACTS_DIR")
		initialize_recommendation_api(
			cache=cache,
			artifacts_dir=Path(artifacts_dir) if artifacts_dir else None,
		)
		logger.info("Redis and recommendation cache initialized")
	except Exception as e:
		logger.warning(f"Redis initialization failed: {e}")


@app.get("/health")
def health() -> dict:
	return {"status": "ok"}


@app.get("/metrics")
def metrics() -> Response:
	return metrics_response()


app.include_router(search.router)
app.include_router(events.router)
app.include_router(recommendations.router)
