import time

from fastapi import Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from starlette.middleware.base import BaseHTTPMiddleware


REQUEST_COUNT = Counter(
    "http_requests_total",
    "Total HTTP requests.",
    ["method", "route", "status_code"],
)

REQUEST_DURATION = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds.",
    ["method", "route"],
)

RESPONSE_SIZE = Histogram(
    "http_response_size_bytes",
    "HTTP response size in bytes.",
    ["method", "route"],
)

BACKEND_ERRORS = Counter(
    "backend_errors_total",
    "Total backend HTTP errors.",
    ["method", "route", "status_code"],
)


def _route_label(request: Request) -> str:
    route = request.scope.get("route")
    if route and getattr(route, "path", None):
        return route.path
    return request.url.path


class PrometheusMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.url.path == "/metrics":
            return await call_next(request)

        start_time = time.perf_counter()
        response = None
        status_code = "500"

        try:
            response = await call_next(request)
            status_code = str(response.status_code)
            return response
        finally:
            route = _route_label(request)
            method = request.method
            duration = time.perf_counter() - start_time
            REQUEST_COUNT.labels(method=method, route=route, status_code=status_code).inc()
            REQUEST_DURATION.labels(method=method, route=route).observe(duration)
            if response is not None:
                content_length = response.headers.get("content-length")
                if content_length and content_length.isdigit():
                    RESPONSE_SIZE.labels(method=method, route=route).observe(int(content_length))
            if status_code.startswith("5"):
                BACKEND_ERRORS.labels(method=method, route=route, status_code=status_code).inc()


def metrics_response() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
