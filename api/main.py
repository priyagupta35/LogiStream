import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

from api.config import settings
from api.database import engine, Base
from api.utils.kafka_producer import producer
from api.utils.redis_client import redis_client
from api.routers import auth, orders, drivers, tracking

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("logistream.api")

# Prometheus Metrics
REQUESTS_TOTAL = Counter(
    "logistream_http_requests_total",
    "Total HTTP Requests",
    ["method", "endpoint", "status"]
)
REQUEST_DURATION = Histogram(
    "logistream_http_request_duration_seconds",
    "HTTP Request Latency in seconds",
    ["endpoint"]
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle event handler for managing service startup and graceful teardown."""
    logger.info("Initializing LogiStream API Service...")

    # 1. Initialize DB tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database schemas initialized.")

    # 2. Connect Kafka Producer
    await producer.start()

    # 3. Connect Redis
    await redis_client.connect()

    yield

    # Teardown
    logger.info("Shutting down LogiStream API Service...")
    await producer.stop()
    await redis_client.disconnect()
    await engine.dispose()
    logger.info("Service connections cleanly closed.")

app = FastAPI(
    title=settings.PROJECT_NAME,
    description="Event-Driven Logistics & Real-Time Delivery Backend Engine.",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Metrics Middleware
@app.middleware("http")
async def monitor_requests(request: Request, call_next):
    endpoint = request.url.path
    with REQUEST_DURATION.labels(endpoint=endpoint).time():
        response: Response = await call_next(request)
        REQUESTS_TOTAL.labels(
            method=request.method,
            endpoint=endpoint,
            status=response.status_code
        ).inc()
        return response

# Prometheus scraping endpoint
@app.get("/metrics", tags=["Observability"])
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

# Health endpoint
@app.get("/health", tags=["Health"])
async def health_check():
    return {
        "status": "HEALTHY",
        "service": "LogiStream API",
        "version": "1.0.0"
    }

# Include Routers
app.include_router(auth.router, prefix=settings.API_V1_STR)
app.include_router(orders.router, prefix=settings.API_V1_STR)
app.include_router(drivers.router, prefix=settings.API_V1_STR)
app.include_router(tracking.router, prefix=settings.API_V1_STR)
