# =============================================================================
# NOTIFICATION ENGINE - REDIS CONNECTION
# =============================================================================
# Redis connection manager - SIRF connection setup
# =============================================================================

import logging
import socket
from typing import Optional

import redis
from redis.exceptions import RedisError

from config.settings import settings

logger = logging.getLogger(__name__)


class RedisConnection:
    """
    Redis connection manager using singleton pattern.
    Provides connection pooling for efficient Redis operations.
    """
    _instance: Optional["RedisConnection"] = None
    _client: Optional[redis.Redis] = None

    def __new__(cls) -> "RedisConnection":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if self._client is None:
            self._client = self._create_client()

    def _create_client(self) -> redis.Redis:
        """Create Redis client with connection pool."""
        pool_config = {
            "host": settings.REDIS_HOST,
            "port": settings.REDIS_PORT,
            "db": settings.REDIS_DB,
            "decode_responses": settings.REDIS_DECODE_RESPONSES,
            "max_connections": settings.REDIS_MAX_CONNECTIONS,
            "socket_timeout": settings.REDIS_SOCKET_TIMEOUT,
            "socket_connect_timeout": settings.REDIS_SOCKET_CONNECT_TIMEOUT,
            "socket_keepalive": True,
            "retry_on_timeout": True,
            "health_check_interval": 30,
        }

        if settings.REDIS_PASSWORD:
            pool_config["password"] = settings.REDIS_PASSWORD

        pool = redis.ConnectionPool(**pool_config)
        client = redis.Redis(connection_pool=pool)
        logger.info(f"Redis client created: {settings.REDIS_HOST}:{settings.REDIS_PORT}")
        return client

    @property
    def client(self) -> redis.Redis:
        """Get Redis client instance."""
        return self._client

    def ping(self) -> bool:
        """Check if Redis connection is alive."""
        try:
            return self._client.ping()
        except RedisError as e:
            logger.error(f"Redis ping failed: {e}")
            return False

    def close(self) -> None:
        """Close Redis connection."""
        if self._client:
            self._client.close()
            self._client = None
            RedisConnection._instance = None
            logger.info("Redis connection closed")


# Global Redis connection instance
redis_connection = RedisConnection()


def get_redis() -> redis.Redis:
    """Dependency function to get Redis client."""
    return redis_connection.client


def check_redis_connection() -> bool:
    """Check if Redis connection is healthy."""
    return redis_connection.ping()
