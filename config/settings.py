# =============================================================================
# NOTIFICATION ENGINE - SETTINGS
# =============================================================================

from typing import Optional
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # -------------------------------------------------------------------------
    # APPLICATION
    # -------------------------------------------------------------------------
    APP_NAME: Optional[str] = "NotificationEngine"
    APP_ENV: Optional[str] = "development"
    DEBUG: Optional[bool] = False
    LOG_LEVEL: Optional[str] = "INFO"

    ENGINE_HOST: Optional[str] = "0.0.0.0"
    ENGINE_PORT: Optional[int] = 8001

    # -------------------------------------------------------------------------
    # DATABASE
    # -------------------------------------------------------------------------
    DB_DRIVER: Optional[str] = "psycopg2"
    DB_CONNECTION: Optional[str] = "postgresql"
    DB_HOST: Optional[str] = "localhost"
    DB_PORT: Optional[int] = 5432
    DB_DATABASE: Optional[str] = "nextgen_development"
    DB_USERNAME: Optional[str] = "devuser"
    DB_PASSWORD: Optional[str] = None

    DB_POOL_SIZE: Optional[int] = 10
    DB_MAX_OVERFLOW: Optional[int] = 20
    DB_POOL_TIMEOUT: Optional[int] = 30
    DB_POOL_RECYCLE: Optional[int] = 3600

    # -------------------------------------------------------------------------
    # REDIS
    # -------------------------------------------------------------------------
    REDIS_HOST: Optional[str] = "localhost"
    REDIS_PORT: Optional[int] = 6379
    REDIS_DB: Optional[int] = 0
    REDIS_PASSWORD: Optional[str] = None
    REDIS_DECODE_RESPONSES: Optional[bool] = True

    REDIS_MAX_CONNECTIONS: Optional[int] = 20
    REDIS_SOCKET_TIMEOUT: Optional[int] = 5
    REDIS_SOCKET_CONNECT_TIMEOUT: Optional[int] = 5

    REDIS_PREFIX_ONLINE: Optional[str] = "online:user"
    REDIS_PREFIX_SESSION: Optional[str] = "session"
    REDIS_ONLINE_TTL: Optional[int] = 86400

    # -------------------------------------------------------------------------
    # RABBITMQ
    # -------------------------------------------------------------------------
    RABBITMQ_HOST: Optional[str] = "localhost"
    RABBITMQ_PORT: Optional[int] = 5672
    RABBITMQ_USER: Optional[str] = "guest"
    RABBITMQ_PASS: Optional[str] = "guest"
    RABBITMQ_VHOST: Optional[str] = "/"

    RABBITMQ_HEARTBEAT: Optional[int] = 600
    RABBITMQ_BLOCKED_TIMEOUT: Optional[int] = 300
    RABBITMQ_CONNECTION_ATTEMPTS: Optional[int] = 5
    RABBITMQ_RETRY_DELAY: Optional[int] = 2

    RABBITMQ_EXCHANGE: Optional[str] = "notifications.exchange"
    RABBITMQ_QUEUE: Optional[str] = "notifications.process"
    RABBITMQ_ROUTING_KEY: Optional[str] = "notification"
    RABBITMQ_PREFETCH_COUNT: Optional[int] = 20

    # -------------------------------------------------------------------------
    # JWT / AUTH
    # -------------------------------------------------------------------------
    SECRET_KEY: Optional[str] = None
    ALGORITHM: Optional[str] = "HS256"

    # -------------------------------------------------------------------------
    # WEBSOCKET
    # -------------------------------------------------------------------------
    WEBSOCKET_CORS_ORIGINS: Optional[str] = "*"
    WEBSOCKET_PING_TIMEOUT: Optional[int] = 60
    WEBSOCKET_PING_INTERVAL: Optional[int] = 25

    # -------------------------------------------------------------------------
    # LOGGING
    # -------------------------------------------------------------------------
    LOG_FILE: Optional[str] = "notification_engine.log"
    LOG_DIRECTORY: Optional[str] = "./logs"
    LOG_RETENTION_DAYS: Optional[int] = 7
    LOG_FORMAT: Optional[str] = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    class Config:
        env_file = ".env"


settings = Settings()
