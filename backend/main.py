from fastapi import FastAPI


app = FastAPI(title="Movie Recommendation API")


@app.get("/health")
def health() -> dict:
	return {"status": "ok"}
