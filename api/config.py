from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    PROJECT_NAME: str = "LogiStream"
    API_V1_STR: str = "/api/v1"
    SECRET_KEY: str = "logistream-super-secure-secret-key-change-in-production-2026"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24  # 1 day

    # Database
    DATABASE_URL: str = "postgresql+asyncpg://logistream:logistream123@localhost:5432/logistream_db"
    SYNC_DATABASE_URL: str = "postgresql://logistream:logistream123@localhost:5432/logistream_db"

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_ORDER_EVENTS_TOPIC: str = "orders.events"
    KAFKA_DELIVERY_EVENTS_TOPIC: str = "delivery.events"
    KAFKA_ORDER_RETRY_TOPIC: str = "orders.retry"
    KAFKA_ORDER_DLQ_TOPIC: str = "orders.dlq"
    KAFKA_CONSUMER_GROUP_DISPATCH: str = "dispatch-service-group"
    KAFKA_CONSUMER_GROUP_TRACKING: str = "tracking-service-group"
    KAFKA_CONSUMER_GROUP_NOTIFY: str = "notifications-service-group"

    # Redis
    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_DRIVER_GEO_KEY: str = "drivers:locations"
    REDIS_DRIVER_STATUS_KEY: str = "drivers:status"

    # Fallback simulation mode if external brokers are booting
    ENABLE_IN_MEMORY_FALLBACK: bool = False

    class Config:
        env_file = ".env"
        extra = "allow"

settings = Settings()
