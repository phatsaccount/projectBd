from fastapi import FastAPI
from backend.app.interfaces.api.routes import search


app = FastAPI(title="Movie Recommendation API")


@app.get("/health")
def health() -> dict:
	return {"status": "ok"}


app.include_router(search.router)
