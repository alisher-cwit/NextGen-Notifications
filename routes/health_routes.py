"""Health Check Routes - System health monitoring endpoints."""

from fastapi import APIRouter, HTTPException

from config.settings import settings
from config.logging_config import get_logger
from database import check_database_connection
from redis_manager import check_redis_connection

router = APIRouter(tags=["Health"])
logger = get_logger(__name__)

# Consumer reference (set from main.py)
_consumer = None


def set_consumer(consumer):
    """Set RabbitMQ consumer reference for health checks."""
    global _consumer
    _consumer = consumer


@router.get("/health")
async def health_check():
    """Health check - returns status of all components."""
    db_healthy = check_database_connection()
    redis_healthy = check_redis_connection()
    consumer_healthy = _consumer.is_running if _consumer else False

    all_healthy = db_healthy and redis_healthy and consumer_healthy

    status = {
        "status": "healthy" if all_healthy else "unhealthy",
        "components": {
            "database": "up" if db_healthy else "down",
            "redis": "up" if redis_healthy else "down",
            "rabbitmq_consumer": "running" if consumer_healthy else "stopped",
            "websocket": "up",
        },
        "environment": settings.APP_ENV,
    }

    if not all_healthy:
        raise HTTPException(status_code=503, detail=status)

    return status


@router.get("/health/live")
async def liveness_check():
    """Kubernetes liveness probe."""
    return {"status": "alive"}


@router.get("/health/ready")
async def readiness_check():
    """Kubernetes readiness probe."""
    if not check_database_connection():
        raise HTTPException(status_code=503, detail="Database not ready")
    if not check_redis_connection():
        raise HTTPException(status_code=503, detail="Redis not ready")
    return {"status": "ready"}


@router.get("/stats")
async def get_stats():
    """Get notification engine statistics."""
    from websocket.manager import connection_manager

    return {
        "websocket": connection_manager.get_stats(),
        "consumer": _consumer.get_stats() if _consumer else {"running": False},
    }


@router.get("/queue-stats")
async def get_queue_stats():
    """Get RabbitMQ queue statistics."""
    if not _consumer:
        raise HTTPException(status_code=503, detail="Consumer not initialized")

    stats = _consumer.get_stats()
    return {
        "queue": stats["queue"],
        "processing": {
            "total_consumed": stats["total_consumed"],
            "total_processed": stats["total_processed"],
            "total_failed": stats["total_failed"],
            "success_rate": stats["success_rate"],
        }
    }
