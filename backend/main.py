from fastapi import FastAPI
from backend.app.interfaces.api.routes import search, events, recommendations
from backend.app.interfaces.api.routes.events import initialize_producer
from backend.app.interfaces.api.routes.recommendations import initialize_recommendation_api
from backend.app.infrastructure.redis.client import initialize_redis
from backend.app.infrastructure.redis.recommendation_cache import initialize_recommendation_cache
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Movie Recommendation API")


@app.on_event("startup")
def startup_event():
	"""Initialize producers and connections on startup."""
	try:
		# Initialize event producer
		initialize_producer(bootstrap_servers="localhost:9092")
		logger.info("Event producer initialized")
	except Exception as e:
		logger.warning(f"Event producer initialization failed: {e}")
	
	try:
		# Initialize Redis client and recommendation cache
		redis_client = initialize_redis(host="localhost", port=6379, db=0)
		cache = initialize_recommendation_cache(redis_client, ttl=3600)
		
		# Initialize recommendation API
		initialize_recommendation_api(cache=cache)
		logger.info("Redis and recommendation cache initialized")
	except Exception as e:
		logger.warning(f"Redis initialization failed: {e}")


@app.get("/health")
def health() -> dict:
	return {"status": "ok"}


app.include_router(search.router)
app.include_router(events.router)
app.include_router(recommendations.router)
